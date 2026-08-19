import SwiftUI

@main
struct CorePropApp: App {
    var body: some Scene {
        WindowGroup {
            RootView()
                .preferredColorScheme(.dark)
        }
    }
}

struct RootView: View {
    @StateObject private var model = WebViewModel()

    var body: some View {
        ZStack {
            // Painted behind everything so the notch and home-indicator strips
            // match the page instead of showing system black.
            Theme.background.ignoresSafeArea()

            WebView(model: model)
                // Top safe area is NOT ignored on purpose: the site's .cp-nav
                // is `position:sticky; top:0` with no env(safe-area-inset-top)
                // padding, so letting the web view run under the notch would
                // put the tab bar behind the status bar.
                .ignoresSafeArea(edges: .bottom)
                .opacity(model.errorMessage == nil ? 1 : 0)

            if model.isInitialLoad && model.errorMessage == nil {
                ProgressView()
                    .progressViewStyle(.circular)
                    .tint(Theme.accent)
                    .scaleEffect(1.3)
            }

            if let message = model.errorMessage {
                ErrorView(message: message) { model.reload() }
            }
        }
    }
}

struct ErrorView: View {
    let message: String
    let retry: () -> Void

    var body: some View {
        ZStack {
            Theme.background.ignoresSafeArea()

            VStack(spacing: 16) {
                Image(systemName: "wifi.exclamationmark")
                    .font(.system(size: 44, weight: .light))
                    .foregroundStyle(Theme.secondaryText)

                Text(message)
                    .font(.system(size: 15))
                    .foregroundStyle(Theme.secondaryText)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 40)

                Button(action: retry) {
                    Text("Try again")
                        .font(.system(size: 15, weight: .semibold))
                        .foregroundStyle(.white)
                        .padding(.horizontal, 26)
                        .padding(.vertical, 12)
                        .background(Theme.accent, in: RoundedRectangle(cornerRadius: 10))
                }
                .padding(.top, 4)
            }
        }
    }
}
