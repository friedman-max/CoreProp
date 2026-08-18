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
            VStack(spacing: 22) {
                Spacer(minLength: 40)
                VStack(spacing: 10) {
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
                        .padding(.horizontal, 28)
                }

                card
                Spacer(minLength: 24)
                Text("Responsible gaming: this is an analytics tool, not betting advice. 21+. If gambling stops being fun, call 1-800-GAMBLER.")
                    .font(Theme.ui(11))
                    .foregroundColor(Theme.text3)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 28)
                    .padding(.bottom, 20)
            }
            .frame(maxWidth: .infinity)
        }
        .background(Theme.bg.ignoresSafeArea())
    }

    private var card: some View {
        VStack(spacing: 16) {
            Picker("", selection: $mode) {
                Text("Log in").tag(Mode.signIn)
                Text("Create account").tag(Mode.signUp)
            }
            .pickerStyle(.segmented)

            if auth.pendingEmailConfirmation {
                confirmationNotice
            }

            VStack(spacing: 10) {
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
                    .foregroundColor(Color(hex: 0xFCA5A5))
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
        .cpCard(radius: 18, padding: 20)
        .padding(.horizontal, 20)
    }

    private var confirmationNotice: some View {
        HStack(spacing: 8) {
            Image(systemName: "envelope.badge").foregroundColor(Theme.primary2)
            Text("Check your email to confirm your account, then log in.")
                .font(Theme.ui(13)).foregroundColor(Theme.text2)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
        .background(Theme.primaryHi)
        .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
    }

    private var canSubmit: Bool {
        !email.trimmingCharacters(in: .whitespaces).isEmpty && password.count >= 6 && auth.isConfigured
    }

    @ViewBuilder
    private func field(title: String, text: Binding<String>, placeholder: String,
                       field: Field, keyboard: UIKeyboardType, content: UITextContentType?) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(title).font(Theme.ui(12, .semibold)).foregroundColor(Theme.text3)
            TextField(placeholder, text: text)
                .textInputAutocapitalization(.never)
                .autocorrectionDisabled()
                .keyboardType(keyboard)
                .textContentType(content)
                .focused($focus, equals: field)
                .padding(12)
                .background(Theme.inputBg)
                .foregroundColor(Theme.text)
                .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
                .overlay(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous)
                    .stroke(focus == field ? Theme.primary : Theme.hair, lineWidth: 1))
        }
    }

    @ViewBuilder
    private func secureField(title: String, text: Binding<String>) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(title).font(Theme.ui(12, .semibold)).foregroundColor(Theme.text3)
            SecureField("••••••••", text: text)
                .textContentType(mode == .signUp ? .newPassword : .password)
                .focused($focus, equals: .password)
                .padding(12)
                .background(Theme.inputBg)
                .foregroundColor(Theme.text)
                .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
                .overlay(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous)
                    .stroke(focus == .password ? Theme.primary : Theme.hair, lineWidth: 1))
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
