import SwiftUI
import CorePropKit

/// Subscription status — a **reader/companion** screen. Per App Store Guideline
/// 3.1.1 it does not present an in-app purchase or a link to buy on the web; it
/// shows status and, for an existing paying subscriber, a "Manage subscription"
/// affordance (the Stripe portal). Billing is off by default, so most users see
/// "full access".
struct SubscriptionView: View {
    @EnvironmentObject private var model: AppModel
    @Environment(\.openURL) private var openURL
    @State private var openingPortal = false
    @State private var portalError: String?

    private var billing: BillingStatus? { model.billing }

    var body: some View {
        ScrollView {
            VStack(spacing: Theme.s4) {
                statusCard
                if let b = billing, b.isUnlocked, !b.isComped {
                    manageButton
                } else if let b = billing, !b.isUnlocked, b.enforce == true {
                    lockedNotice
                }
                disclaimer
            }
            .padding(Theme.s4)
        }
        .background(Theme.bg.ignoresSafeArea())
        .navigationTitle("Subscription")
        .navigationBarTitleDisplayMode(.inline)
        .task { await model.refreshBilling() }
    }

    private var statusCard: some View {
        VStack(alignment: .leading, spacing: Theme.s3) {
            HStack {
                // tracking, not kerning: tracking is the letter-spacing
                // analogue, kerning adjusts glyph pairs. Web's micro-label
                // tracking is .04em, which at 10.5pt is 0.42pt.
                Text("STATUS").font(Theme.ui(10.5, .semibold)).tracking(0.42).foregroundColor(Theme.text3)
                Spacer()
                statusPill
            }
            if let b = billing {
                if let plan = b.plan { row("Plan", plan.capitalized) }
                if b.isTrialing, let end = b.currentPeriodEndDate {
                    row("Trial ends", Fmt.shortDate(end))
                } else if let end = b.currentPeriodEndDate, !b.isComped {
                    row("Renews", Fmt.shortDate(end))
                }
                if b.isComped { row("Access", "Comped — thank you") }
                if b.enforce == false { row("Access", "Full (billing not enforced)") }
            } else {
                Text("Loading…").font(Theme.ui(13)).foregroundColor(Theme.text3)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .cpCard()
    }

    private var statusPill: some View {
        let unlocked = billing?.isUnlocked ?? true
        return Text(unlocked ? "Active" : "Inactive")
            .font(Theme.ui(11, .bold)).foregroundColor(unlocked ? Theme.green2 : Theme.red2)
            // Follows its siblings onto the scale rather than off it: 9 -> s2 (8),
            // the nearest step, and 4 was already s1's value. DataAgePill made the
            // identical 9 -> s2 move, so the two pills still match — which was the
            // real point of the note that used to sit here, and it survives the
            // migration intact.
            .padding(.horizontal, Theme.s2).padding(.vertical, Theme.s1)
            .background(unlocked ? Theme.greenHi : Theme.redHi).clipShape(Capsule())
    }

    private var manageButton: some View {
        VStack(spacing: Theme.s2) {
            Button {
                Task { await openPortal() }
            } label: {
                if openingPortal { ProgressView().tint(.white) }
                else { Label("Manage subscription", systemImage: "arrow.up.forward.square") }
            }
            .buttonStyle(PrimaryButtonStyle())
            .disabled(openingPortal)
            if let portalError {
                Text(portalError).font(Theme.ui(12)).foregroundColor(Theme.red2)
            }
            Text("Opens the secure Stripe portal in Safari to update payment or cancel.")
                .font(Theme.ui(11)).foregroundColor(Theme.text3).multilineTextAlignment(.center)
        }
    }

    private var lockedNotice: some View {
        VStack(spacing: Theme.s2) {
            Image(systemName: "lock").font(.system(size: 26)).foregroundColor(Theme.amber)
            Text("Your subscription is inactive.")
                .font(Theme.ui(15, .semibold)).foregroundColor(Theme.text)
            Text("Manage your CoreProp account from the web to restore access.")
                .font(Theme.ui(13)).foregroundColor(Theme.text3).multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity)
        .cpCard()
    }

    private var disclaimer: some View {
        Text("Subscriptions are managed by CoreProp on the web via Stripe. This app is a companion for viewing your account.")
            .font(Theme.ui(11)).foregroundColor(Theme.text3)
            .multilineTextAlignment(.center)
    }

    private func row(_ label: String, _ value: String) -> some View {
        HStack {
            Text(label).font(Theme.ui(14)).foregroundColor(Theme.text2)
            Spacer()
            Text(value).font(Theme.mono(14, .medium)).foregroundColor(Theme.text)
        }
    }

    private func openPortal() async {
        openingPortal = true
        portalError = nil
        defer { openingPortal = false }
        do {
            let res = try await model.client.billingPortal()
            if let urlStr = res.url, let url = URL(string: urlStr) { openURL(url) }
            else { portalError = "The portal link was unavailable." }
        } catch let e as APIError {
            portalError = e.display
        } catch {
            portalError = error.localizedDescription
        }
    }
}
