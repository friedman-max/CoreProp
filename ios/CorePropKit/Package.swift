// swift-tools-version: 5.9
import PackageDescription

// CorePropKit — the platform-agnostic core of the CoreProp iOS app.
//
// Everything here is Foundation-only (no SwiftUI / UIKit) so it compiles and
// runs on macOS as well as iOS. That is deliberate: it lets the models, the
// networking layer, and the EV / payout math be verified on a plain macOS
// toolchain (`swift run CorePropKitVerify`) without an iOS simulator, which is
// how CI and `Command Line Tools`-only machines validate the logic.
//
// The SwiftUI app target (see ../project.yml) depends on the `CorePropKit`
// library product and imports it. The `CorePropKitVerify` executable is an
// assertion runner used for local verification (XCTest is unavailable under
// bare Command Line Tools, so we do not ship a test target in this package —
// the Xcode project carries the XCTest suite instead).
let package = Package(
    name: "CorePropKit",
    platforms: [
        .iOS(.v16),
        .macOS(.v13),
    ],
    products: [
        .library(name: "CorePropKit", targets: ["CorePropKit"]),
    ],
    targets: [
        .target(
            name: "CorePropKit",
            path: "Sources/CorePropKit"
        ),
        .executableTarget(
            name: "CorePropKitVerify",
            dependencies: ["CorePropKit"],
            path: "Sources/CorePropKitVerify"
        ),
    ]
)
