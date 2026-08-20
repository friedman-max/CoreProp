import SwiftUI
import CorePropKit

/// Switches between the launch splash, the signed-out auth flow, and the main
/// tabbed app based on `AuthManager.phase`.
struct RootView: View {
    @EnvironmentObject private var model: AppModel
    @EnvironmentObject private var auth: AuthManager

    var body: some View {
        ZStack {
            Theme.bg.ignoresSafeArea()
            switch auth.phase {
            case .loading:  SplashView(error: model.bootstrapError)
            case .signedOut: AuthView()
            case .signedIn:  MainTabView()
            }
        }
        .task { await model.bootstrap() }
        .animation(.easeInOut(duration: 0.25), value: auth.phase)
    }
}

/// Branded launch screen while `bootstrap()` loads config + restores the session.
struct SplashView: View {
    var error: String?
    var body: some View {
        // 18 was a tie between s4 (16) and s5 (20); ties round up. The 40pt
        // wordmark height is component geometry, not spacing, and stays put.
        VStack(spacing: Theme.s5) {
            BrandWordmark(height: 40)
            Text(CorePropConstants.tagline)
                .font(Theme.ui(14))
                .foregroundColor(Theme.text3)
            if let error {
                Text(error)
                    .font(Theme.ui(12))
                    .foregroundColor(Theme.amber)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, Theme.s8)
            } else {
                ProgressView()
                    .tint(Theme.primary2)
                    .padding(.top, Theme.s2)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

/// The app wordmark. Uses the bundled brand image when present, falling back to
/// a text lockup so the app never renders a blank header if the asset is missing.
struct BrandWordmark: View {
    var height: CGFloat = 26
    var body: some View {
        if UIImage(named: "BrandWordmark") != nil {
            Image("BrandWordmark")
                .resizable()
                .scaledToFit()
                .frame(height: height)
                .accessibilityLabel(CorePropConstants.appName)
        } else {
            Text(CorePropConstants.appName)
                .font(Theme.ui(height * 0.7, .bold))
                .foregroundColor(Theme.text)
        }
    }
}
