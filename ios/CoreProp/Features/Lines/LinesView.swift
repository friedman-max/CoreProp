import SwiftUI
import CorePropKit

/// The board tabs consolidated: a segmented control for Combined / PrizePicks /
/// Sportsbooks, with a secondary book picker for the Sportsbooks view.
struct LinesView: View {
    @EnvironmentObject private var model: AppModel
    @StateObject private var vm = LinesViewModel()

    private enum Segment: String, CaseIterable { case combined = "Combined", prizepicks = "PrizePicks", sportsbooks = "Sportsbooks" }
    @State private var segment: Segment = .combined
    @State private var book: BoardSource = .fanduel   // used only for .sportsbooks

    private var source: BoardSource {
        switch segment {
        case .combined:    return .combined
        case .prizepicks:  return .prizepicks
        case .sportsbooks: return book
        }
    }

    var body: some View {
        NavigationStack {
            VStack(spacing: 10) {
                Picker("Board", selection: $segment) {
                    ForEach(Segment.allCases, id: \.self) { Text($0.rawValue).tag($0) }
                }
                .pickerStyle(.segmented)
                .padding(.horizontal, 14)

                if segment == .sportsbooks {
                    Picker("Book", selection: $book) {
                        Text("FanDuel").tag(BoardSource.fanduel)
                        Text("DraftKings").tag(BoardSource.draftkings)
                        Text("Pinnacle").tag(BoardSource.pinnacle)
                    }
                    .pickerStyle(.segmented)
                    .padding(.horizontal, 14)
                }

                content
            }
            .padding(.top, 8)
            .background(Theme.bg.ignoresSafeArea())
            .navigationTitle("Lines")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Menu {
                        Picker("Sort", selection: $vm.sortField) {
                            ForEach(LinesViewModel.SortField.allCases, id: \.self) { Text($0.rawValue).tag($0) }
                        }
                    } label: {
                        Image(systemName: "arrow.up.arrow.down")
                            .accessibilityLabel("Sort lines")
                    }
                }
            }
        }
        .searchable(text: $vm.searchText, prompt: "Player or prop")
        .task(id: source) { await vm.load(source: source, client: model.client, model: model) }
    }

    @ViewBuilder
    private var content: some View {
        switch vm.state {
        case .idle, .loading:
            List(0..<8, id: \.self) { _ in
                SkeletonRow().listRowBackground(Color.clear).listRowSeparator(.hidden)
            }
            .listStyle(.plain).scrollContentBackground(.hidden)
        case .failed(let m):
            ScrollView { ErrorStateView(message: m) { Task { await vm.load(source: source, client: model.client, model: model) } } }
        case .empty:
            ScrollView { EmptyStateView(systemImage: "rectangle.on.rectangle.slash",
                                        title: "No lines for this board") }
        case .loaded:
            List {
                if vm.filtered.isEmpty {
                    EmptyStateView(systemImage: "line.3.horizontal.decrease.circle", title: "No lines match")
                        .listRowBackground(Color.clear).listRowSeparator(.hidden)
                } else {
                    ForEach(vm.filtered) { line in
                        LineRow(line: line, source: source)
                            .listRowBackground(Color.clear)
                            .listRowSeparator(.hidden)
                            .listRowInsets(EdgeInsets(top: 2, leading: 8, bottom: 2, trailing: 8))
                    }
                }
            }
            .listStyle(.plain)
            .scrollContentBackground(.hidden)
            .refreshable { await vm.load(source: source, client: model.client, model: model) }
        }
    }
}

/// A single board line. Shows the union of whatever the endpoint carries: for
/// the combined board, per-book chips + best + fair; for single-book boards, one
/// price + fair.
struct LineRow: View {
    let line: MarketLine
    let source: BoardSource

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 8) {
                Text(line.playerName ?? "—").font(Theme.ui(15, .semibold)).foregroundColor(Theme.text).lineLimit(1)
                if let lg = line.league { LeaguePill(league: lg) }
                Spacer()
                if let pct = line.truePct {
                    Text(Fmt.percentValue(pct)).font(Theme.mono(13, .bold)).foregroundColor(Theme.primary2)
                }
            }
            HStack(spacing: 8) {
                if !line.sideLabel.isEmpty { SideBadge(side: line.sideLabel) }
                Text(line.prop).font(Theme.ui(13)).foregroundColor(Theme.text2).lineLimit(1)
                if let v = line.lineValue {
                    Text(Fmt.line(v)).font(Theme.mono(13, .semibold)).foregroundColor(Theme.text)
                }
                if let start = line.startDate {
                    Text("· \(Fmt.gameTime(start))").font(Theme.mono(11)).foregroundColor(Theme.text3).lineLimit(1)
                }
            }
            oddsRow
        }
        .padding(.vertical, 10).padding(.horizontal, 12)
    }

    @ViewBuilder
    private var oddsRow: some View {
        HStack(spacing: 8) {
            if source == .combined {
                bookChip(.fanduel, line.fd)
                bookChip(.draftkings, line.dk)
                bookChip(.pinnacle, line.pin)
                bookChip(.novig, line.nv)
                Spacer()
                if let best = line.best {
                    labelled("BEST", Fmt.americanOdds(best), Theme.green)
                }
                if let t = line.trueOddsInt {
                    labelled("FAIR", Fmt.americanOdds(t), Theme.primary2)
                }
            } else {
                if let odds = line.bookOdds {
                    labelled(source.title.uppercased(), Fmt.americanOdds(odds), Theme.text)
                }
                Spacer()
                if let t = line.trueOddsInt {
                    labelled("FAIR", Fmt.americanOdds(t), Theme.primary2)
                }
            }
        }
    }

    @ViewBuilder
    private func bookChip(_ book: Book, _ odds: Int?) -> some View {
        if let odds {
            HStack(spacing: 3) {
                BookBadgeView(book: book)
                Text(Fmt.americanOdds(odds)).font(Theme.mono(11, .medium)).foregroundColor(Theme.text3)
            }
        }
    }

    private func labelled(_ label: String, _ value: String, _ color: Color) -> some View {
        VStack(alignment: .trailing, spacing: 0) {
            Text(label).font(Theme.ui(8, .semibold)).foregroundColor(Theme.text4)
            Text(value).font(Theme.mono(12, .semibold)).foregroundColor(color)
        }
    }
}
