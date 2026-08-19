import Foundation

enum AppConfig {
    /// Point this at a LAN address (e.g. http://192.168.1.20:8021) plus an ATS
    /// exception in Info.plist if you ever want the app to hit a dev server.
    static let baseURL = URL(string: "https://coreprop.me")!

    /// Hosts that stay INSIDE the app's web view.
    ///
    /// Stripe is on this list deliberately. Checkout is a round trip: the site
    /// hands off to checkout.stripe.com and Stripe redirects back to
    /// coreprop.me on success. If Stripe opened in Safari, the return leg would
    /// land in Safari too — the purchase would complete in a browser this app
    /// cannot see, and the app would sit there still showing the paywall.
    ///
    /// Same reasoning for Supabase: it sets the auth session cookie, and a
    /// cookie set in Safari is invisible to this web view's cookie store.
    private static let inAppHosts: Set<String> = [
        "coreprop.me",
        "www.coreprop.me",
        "checkout.stripe.com",
        "js.stripe.com",
        "hooks.stripe.com",
    ]

    /// True when `url` should load in the app rather than being handed to Safari.
    static func staysInApp(_ url: URL) -> Bool {
        guard let scheme = url.scheme?.lowercased(),
              scheme == "https" || scheme == "http" else { return false }
        guard let host = url.host?.lowercased() else { return false }
        if inAppHosts.contains(host) { return true }
        // Supabase auth/storage live on a per-project subdomain, and the ref can
        // change between environments — match the family, not one hostname.
        if host.hasSuffix(".supabase.co") { return true }
        return false
    }
}
