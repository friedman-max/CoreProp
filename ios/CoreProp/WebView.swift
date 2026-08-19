import SwiftUI
import WebKit
import SafariServices

/// Shared state between the SwiftUI layer and the WKWebView underneath it.
///
/// Deliberately NOT annotated @MainActor. Every caller is already on the main
/// thread — `makeUIView`, and WebKit's navigation/UI delegate callbacks, which
/// UIKit guarantees are delivered on main. Adding the annotation buys nothing
/// and makes the delegate conformances below fight Swift's isolation checking.
final class WebViewModel: ObservableObject {
    /// Only drives the very first paint. Later navigations keep the previous
    /// page on screen rather than flashing a spinner over readable content.
    @Published var isInitialLoad = true
    @Published var errorMessage: String?

    fileprivate weak var webView: WKWebView?

    func reload() {
        errorMessage = nil
        guard let webView else { return }
        // After a failed FIRST load there is no back/forward item to reload —
        // reload() would be a no-op and the retry button would look broken.
        if webView.url == nil {
            isInitialLoad = true
            webView.load(URLRequest(url: AppConfig.baseURL))
        } else {
            webView.reload()
        }
    }

    var canGoBack: Bool { webView?.canGoBack ?? false }
    func goBack() { webView?.goBack() }
}

struct WebView: UIViewRepresentable {
    @ObservedObject var model: WebViewModel

    func makeCoordinator() -> Coordinator { Coordinator(model: model) }

    func makeUIView(context: Context) -> WKWebView {
        let config = WKWebViewConfiguration()
        // The site opens Stripe checkout from a click handler; without this the
        // window.open() call is swallowed and the button does nothing.
        config.defaultWebpagePreferences.allowsContentJavaScript = true
        config.allowsInlineMediaPlayback = true

        let webView = WKWebView(frame: .zero, configuration: config)
        webView.navigationDelegate = context.coordinator
        webView.uiDelegate = context.coordinator
        webView.allowsBackForwardNavigationGestures = true

        // Without all three of these the web view paints white for a frame
        // before the page's own dark background lands — very visible against a
        // #0a0a0d app on every launch and every navigation.
        webView.isOpaque = false
        webView.backgroundColor = .clear
        webView.scrollView.backgroundColor = .clear

        // Matches the site's own overscroll colour so a rubber-band pull past
        // the top or bottom does not expose a grey system void.
        webView.scrollView.indicatorStyle = .white

        let refresh = UIRefreshControl()
        refresh.tintColor = .white
        refresh.addTarget(
            context.coordinator,
            action: #selector(Coordinator.handleRefresh(_:)),
            for: .valueChanged
        )
        webView.scrollView.refreshControl = refresh

        model.webView = webView
        webView.load(URLRequest(url: AppConfig.baseURL))
        return webView
    }

    func updateUIView(_ webView: WKWebView, context: Context) {}

    final class Coordinator: NSObject, WKNavigationDelegate, WKUIDelegate {
        private let model: WebViewModel

        init(model: WebViewModel) {
            self.model = model
        }

        @objc func handleRefresh(_ sender: UIRefreshControl) {
            model.reload()
        }

        // MARK: Navigation routing

        func webView(
            _ webView: WKWebView,
            decidePolicyFor navigationAction: WKNavigationAction,
            decisionHandler: @escaping (WKNavigationActionPolicy) -> Void
        ) {
            guard let url = navigationAction.request.url else {
                decisionHandler(.allow)
                return
            }

            // tel:, mailto:, maps: and friends — hand straight to the system.
            if let scheme = url.scheme?.lowercased(), scheme != "http", scheme != "https" {
                UIApplication.shared.open(url)
                decisionHandler(.cancel)
                return
            }

            if AppConfig.staysInApp(url) {
                decisionHandler(.allow)
                return
            }

            // Anything else (PrizePicks, sportsbooks, docs) is somebody else's
            // site — show it in a Safari sheet the user can dismiss, rather
            // than stranding them in a chrome-less web view with no way back.
            presentSafari(url)
            decisionHandler(.cancel)
        }

        /// target="_blank" links have no frame to load into, so WKWebView asks
        /// the UI delegate what to do. Returning nil without handling the URL
        /// is why "open in new tab" links silently do nothing in naive wrappers.
        func webView(
            _ webView: WKWebView,
            createWebViewWith configuration: WKWebViewConfiguration,
            for navigationAction: WKNavigationAction,
            windowFeatures: WKWindowFeatures
        ) -> WKWebView? {
            if let url = navigationAction.request.url {
                if AppConfig.staysInApp(url) {
                    webView.load(URLRequest(url: url))
                } else {
                    presentSafari(url)
                }
            }
            return nil
        }

        private func presentSafari(_ url: URL) {
            guard let scene = UIApplication.shared.connectedScenes
                    .compactMap({ $0 as? UIWindowScene })
                    .first(where: { $0.activationState == .foregroundActive }),
                  var top = scene.keyWindow?.rootViewController else { return }
            while let presented = top.presentedViewController { top = presented }

            let safari = SFSafariViewController(url: url)
            safari.preferredBarTintColor = UIColor(Theme.background)
            safari.preferredControlTintColor = UIColor(Theme.accent)
            top.present(safari, animated: true)
        }

        // MARK: Load lifecycle

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            model.isInitialLoad = false
            model.errorMessage = nil
            webView.scrollView.refreshControl?.endRefreshing()
        }

        func webView(
            _ webView: WKWebView,
            didFailProvisionalNavigation navigation: WKNavigation!,
            withError error: Error
        ) {
            handle(error, on: webView)
        }

        func webView(
            _ webView: WKWebView,
            didFail navigation: WKNavigation!,
            withError error: Error
        ) {
            handle(error, on: webView)
        }

        private func handle(_ error: Error, on webView: WKWebView) {
            webView.scrollView.refreshControl?.endRefreshing()
            model.isInitialLoad = false

            let nsError = error as NSError
            // -999 is "cancelled", which is what every redirect and every
            // superseded load reports. Surfacing it as a failure would throw an
            // error screen over a page that is loading perfectly well.
            guard nsError.code != NSURLErrorCancelled else { return }

            switch nsError.code {
            case NSURLErrorNotConnectedToInternet:
                model.errorMessage = "No internet connection."
            case NSURLErrorTimedOut:
                model.errorMessage = "The server took too long to respond."
            case NSURLErrorCannotFindHost, NSURLErrorCannotConnectToHost:
                model.errorMessage = "Can't reach coreprop.me right now."
            default:
                model.errorMessage = error.localizedDescription
            }
        }
    }
}
