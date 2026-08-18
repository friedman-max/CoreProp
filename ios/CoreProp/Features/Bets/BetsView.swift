import SwiftUI
import CorePropKit

/// The +EV Bets board: filters + a sortable list. Tapping a row opens detail;
/// the trailing control adds/removes the bet from the slip.
struct BetsView: View {
    @EnvironmentObject private var model: AppModel
    @EnvironmentObject private var slip: SlipStore
    @StateObject private var vm = BetsViewModel()
    @State private var slipFullAlert = false

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                filterBar
                content
            }
            .background(Theme.bg.ignoresSafeArea())
            .navigationTitle("+EV Bets")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    if vm.lastRefresh != nil { DataAgePill(date: vm.lastRefresh) }
                }
            }
        }
        .searchable(text: $vm.searchText, prompt: "Player or prop")
        .task { await vm.load(client: model.client, model: model) }
        .alert("Slip is full", isPresented: $slipFullAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("A PrizePicks slip holds up to \(SlipStore.maxLegs) legs. Remove one to add another.")
        }
    }

    @ViewBuilder
    private var content: some View {
        switch vm.state {
        case .idle, .loading:
            List(0..<8, id: \.self) { _ in
                SkeletonRow().listRowBackground(Color.clear).listRowSeparator(.hidden)
            }
            .listStyle(.plain)
            .scrollContentBackground(.hidden)
        case .failed(let message):
            ScrollView { ErrorStateView(message: message) { Task { await vm.load(client: model.client, model: model) } } }
        case .empty:
            ScrollView { EmptyStateView(systemImage: "sparkles", title: "No bets right now",
                                        message: "The board refreshes on the server's schedule. Pull to refresh.") }
                .refreshable { await vm.load(client: model.client, model: model) }
        case .loaded:
            loadedList
        }
    }

    private var loadedList: some View {
        List {
            if vm.filtered.isEmpty {
                EmptyStateView(systemImage: "line.3.horizontal.decrease.circle",
                               title: "No bets match your filters")
                    .listRowBackground(Color.clear).listRowSeparator(.hidden)
            } else {
                ForEach(vm.filtered) { bet in
                    NavigationLink { BetDetailView(bet: bet) } label: {
                        BetRow(bet: bet,
                               logged: vm.isLogged(bet),
                               selected: slip.contains(bet),
                               onToggleSlip: { toggle(bet) })
                    }
                    .listRowBackground(Color.clear)
                    .listRowSeparator(.hidden)
                    .listRowInsets(EdgeInsets(top: 2, leading: 8, bottom: 2, trailing: 8))
                }
            }
        }
        .listStyle(.plain)
        .scrollContentBackground(.hidden)
        .refreshable { await vm.load(client: model.client, model: model) }
    }

    private func toggle(_ bet: Bet) {
        if !slip.toggle(bet) { slipFullAlert = true }
    }

    // MARK: Filters

    private var filterBar: some View {
        VStack(spacing: 10) {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    FilterChip(title: "All", selected: vm.selectedLeagues.isEmpty) {
                        vm.selectedLeagues.removeAll(); vm.pruneProps()
                    }
                    ForEach(vm.availableLeagues, id: \.self) { lg in
                        FilterChip(title: lg, selected: vm.selectedLeagues.contains(lg),
                                   accent: Theme.leagueColor(lg)) {
                            if vm.selectedLeagues.contains(lg) { vm.selectedLeagues.remove(lg) }
                            else { vm.selectedLeagues.insert(lg) }
                            vm.pruneProps()
                        }
                    }
                }
                .padding(.horizontal, 14)
            }

            if vm.availableProps.count > 1 {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        FilterChip(title: "All props", selected: vm.selectedProps.isEmpty) {
                            vm.selectedProps.removeAll()
                        }
                        ForEach(vm.availableProps, id: \.self) { p in
                            FilterChip(title: p, selected: vm.selectedProps.contains(p)) {
                                if vm.selectedProps.contains(p) { vm.selectedProps.remove(p) }
                                else { vm.selectedProps.insert(p) }
                            }
                        }
                    }
                    .padding(.horizontal, 14)
                }
            }

            HStack(spacing: 10) {
                HStack(spacing: 6) {
                    Text("Min true")
                        .font(Theme.ui(11, .semibold)).foregroundColor(Theme.text3)
                    Stepper(value: $vm.minTruePct, in: 50...75, step: 1) {
                        Text("\(Int(vm.minTruePct))%")
                            .font(Theme.mono(13, .semibold)).foregroundColor(Theme.text)
                            .frame(minWidth: 40, alignment: .leading)
                    }
                    .labelsHidden()
                    .fixedSize()
                }

                Spacer()

                Menu {
                    Button { vm.sortDescending.toggle() } label: {
                        Label(vm.sortDescending ? "Highest true % first" : "Lowest true % first",
                              systemImage: "arrow.up.arrow.down")
                    }
                    Toggle("Include goblins", isOn: $vm.includeGreenDevils)
                    Toggle("Hide logged", isOn: $vm.hideLogged)
                    Button(role: .destructive) { vm.resetFilters() } label: {
                        Label("Reset filters", systemImage: "xmark.circle")
                    }
                } label: {
                    Image(systemName: "slider.horizontal.3")
                        .font(.system(size: 15, weight: .semibold))
                        .foregroundColor(Theme.primary2)
                        .frame(width: 34, height: 34)
                        .background(Theme.controlBg)
                        .clipShape(RoundedRectangle(cornerRadius: Theme.radiusXs, style: .continuous))
                        .accessibilityLabel("Filters and sorting")
                }
            }
            .padding(.horizontal, 14)
        }
        .padding(.vertical, 10)
        .background(Theme.bg2)
        .overlay(alignment: .bottom) { Divider().overlay(Theme.hair) }
    }
}
