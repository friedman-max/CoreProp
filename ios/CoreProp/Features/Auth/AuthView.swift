import SwiftUI
import CorePropKit

/// Signed-out surface: a branded header over a Log in / Create account form,
/// mirroring the web auth modal (segmented toggle, email + password, username
/// on sign-up). Username rules match the backend: 2–20 chars, letters/numbers/
/// underscore.
struct AuthView: View {
    @EnvironmentObject private var auth: AuthManager
    @EnvironmentObject private var model: AppModel

    private enum Mode { case signIn, signUp }
    @State private var mode: Mode = .signIn
    @State private var email = ""
    @State private var password = ""
    @State private var username = ""
    @State private var busy = false
    @State private var errorMessage: String?
    @FocusState private var focus: Field?

    private enum Field { case email, password, username }

    var body: some View {
        ScrollView {
            // 22 and 10 were both ties (s5/s6 and s2/s3); ties round up.
            VStack(spacing: Theme.s6) {
                Spacer(minLength: Theme.s10)
                VStack(spacing: Theme.s3) {
                    BrandWordmark(height: 38)
                    Text(CorePropConstants.tagline)
                        .font(Theme.ui(15))
                        .foregroundColor(Theme.text3)
                }

                if let coverage = model.coverage {
                    Text("\(coverage.propSource ?? "PrizePicks") props, devigged across \(coverage.booksCountPhrase).")
                        .font(Theme.ui(13))
                        .foregroundColor(Theme.text3)
                        .multilineTextAlignment(.center)
                        .padding(.horizontal, Theme.s8)
                }

                card
                Spacer(minLength: Theme.s6)
                Text("Responsible gaming: this is an analytics tool, not betting advice. 21+. If gambling stops being fun, call 1-800-GAMBLER.")
                    .font(Theme.ui(11))
                    .foregroundColor(Theme.text3)
                    .multilineTextAlignment(.center)
                    // 28 was a tie between s6 (24) and s8 (32); ties round up.
                    .padding(.horizontal, Theme.s8)
                    .padding(.bottom, Theme.s5)
            }
            .frame(maxWidth: .infinity)
        }
        .background(Theme.bg.ignoresSafeArea())
    }

    private var card: some View {
        VStack(spacing: Theme.s4) {
            Picker("", selection: $mode) {
                Text("Log in").tag(Mode.signIn)
                Text("Create account").tag(Mode.signUp)
            }
            .pickerStyle(.segmented)

            if auth.pendingEmailConfirmation {
                confirmationNotice
            }

            VStack(spacing: Theme.s3) {
                if mode == .signUp {
                    field(title: "Username (optional)", text: $username,
                          placeholder: "sharpbettor", field: .username,
                          keyboard: .default, content: .username)
                }
                field(title: "Email", text: $email, placeholder: "you@example.com",
                      field: .email, keyboard: .emailAddress, content: .emailAddress)
                secureField(title: "Password", text: $password)
            }

            if let errorMessage {
                Text(errorMessage)
                    .font(Theme.ui(13))
                    .foregroundColor(Theme.red2)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }

            Button {
                Task { await submit() }
            } label: {
                if busy { ProgressView().tint(.white) }
                else { Text(mode == .signIn ? "Sign in" : "Create account") }
            }
            .buttonStyle(PrimaryButtonStyle())
            .disabled(busy || !canSubmit)

            Button(mode == .signIn ? "New here? Create an account" : "Already have an account? Log in") {
                withAnimation { mode = (mode == .signIn) ? .signUp : .signIn; errorMessage = nil }
            }
            .font(Theme.ui(13, .semibold))
            .foregroundColor(Theme.primary2)
        }
        // 18 was off the radius scale entirely (between --r-lg 16 and --r-xl
        // 20); the card is the app's default card, so it takes rLg like the rest.
        .cpCard(radius: Theme.rLg, padding: Theme.s5)
        .padding(.horizontal, Theme.s5)
    }

    private var confirmationNotice: some View {
        HStack(spacing: Theme.s2) {
            Image(systemName: "envelope.badge").foregroundColor(Theme.primary2)
            Text("Check your email to confirm your account, then log in.")
                .font(Theme.ui(13)).foregroundColor(Theme.text2)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(Theme.s3)
        .background(Theme.primaryHi)
        .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
    }

    private var canSubmit: Bool {
        !email.trimmingCharacters(in: .whitespaces).isEmpty && password.count >= 6 && auth.isConfigured
    }

    @ViewBuilder
    private func field(title: String, text: Binding<String>, placeholder: String,
                       field: Field, keyboard: UIKeyboardType, content: UITextContentType?) -> some View {
        VStack(alignment: .leading, spacing: Theme.s2) {
            Text(title).font(Theme.ui(12, .semibold)).foregroundColor(Theme.text3)
            TextField(placeholder, text: text)
                .textInputAutocapitalization(.never)
                .autocorrectionDisabled()
                .keyboardType(keyboard)
                .textContentType(content)
                .focused($focus, equals: field)
                .padding(Theme.s3)
                .background(Theme.inputBg)
                .foregroundColor(Theme.text)
                .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
                .overlay(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous)
                    .stroke(focus == field ? Theme.primary : Theme.hair, lineWidth: 1))
                .overlay(focusRing(active: focus == field))
        }
    }

    /// Web's `.cp-input:focus` adds `--ring` (`0 0 0 4px var(--primary-hi)`) on
    /// top of the border-colour change; iOS had only the border change. CSS's
    /// spread grows *outward* from the box, so this draws the band entirely
    /// outside the field's bounds: a negative pad expands the shape by
    /// `ringWidth`, `strokeBorder` keeps the stroke inside that expanded frame,
    /// and the outer corner radius is `radiusSm + ringWidth` so the ring stays
    /// concentric with the 1px border it sits around. Not clipped by anything —
    /// the 4pt band lands inside the card's `s5` padding.
    private func focusRing(active: Bool) -> some View {
        RoundedRectangle(cornerRadius: Theme.radiusSm + Theme.ringWidth, style: .continuous)
            .strokeBorder(active ? Theme.primaryHi : Color.clear, lineWidth: Theme.ringWidth)
            .padding(-Theme.ringWidth)
    }

    @ViewBuilder
    private func secureField(title: String, text: Binding<String>) -> some View {
        VStack(alignment: .leading, spacing: Theme.s2) {
            Text(title).font(Theme.ui(12, .semibold)).foregroundColor(Theme.text3)
            SecureField("••••••••", text: text)
                .textContentType(mode == .signUp ? .newPassword : .password)
                .focused($focus, equals: .password)
                .padding(Theme.s3)
                .background(Theme.inputBg)
                .foregroundColor(Theme.text)
                .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
                .overlay(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous)
                    .stroke(focus == .password ? Theme.primary : Theme.hair, lineWidth: 1))
                .overlay(focusRing(active: focus == .password))
        }
    }

    private func submit() async {
        errorMessage = nil
        let cleanEmail = email.trimmingCharacters(in: .whitespaces)
        if mode == .signUp, !username.isEmpty, !isValidUsername(username) {
            errorMessage = "Username must be 2–20 letters, numbers, or underscores."
            return
        }
        busy = true
        defer { busy = false }
        do {
            if mode == .signIn {
                try await auth.signIn(email: cleanEmail, password: password)
            } else {
                let needsConfirm = try await auth.signUp(
                    email: cleanEmail, password: password,
                    username: username.isEmpty ? nil : username)
                if needsConfirm { mode = .signIn }
            }
            await model.refreshBilling()
            await model.registerPushTokenIfNeeded()
        } catch let e as APIError {
            errorMessage = e.errorDescription
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func isValidUsername(_ s: String) -> Bool {
        let count = s.count
        guard count >= 2, count <= 20 else { return false }
        return s.allSatisfy { $0.isLetter || $0.isNumber || $0 == "_" }
    }
}
