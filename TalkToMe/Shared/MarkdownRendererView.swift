import SwiftUI

struct MarkdownRendererView: View {
    private enum Layout {
        static let defaultTopPadding: CGFloat = 30
        static let dividerPadding: CGFloat = 30
        static let paragraphAfterFirstHeadingPadding: CGFloat = 22
        static let headingFontSize: CGFloat = 24
        static let dividerHeight: CGFloat = 1
        static let dividerOpacity: CGFloat = 0.1
    }

    private enum Block {
        case heading(text: String)
        case unorderedList(items: [String])
        case orderedList(items: [String])
        case paragraph(String)
        case rule
        case quote(String)
    }

    private let blocks: [Block]
    private let firstHeadingIndex: Int?

    init(markdown: String) {
        let parsed = Self.parseBlocks(markdown)
        self.blocks = parsed
        self.firstHeadingIndex = parsed.firstIndex { block in
            if case .heading = block { return true }
            return false
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            ForEach(blocks.indices, id: \.self) { index in
                let block = blocks[index]
                let previous: Block? = index > 0 ? blocks[index - 1] : nil
                let top = topPadding(previous: previous, current: block, index: index)

                switch block {
                case .heading(let text):
                    let showDivider = shouldInsertDivider(beforeHeadingAt: index, previous: previous)
                    VStack(alignment: .leading, spacing: 0) {
                        if showDivider {
                            dividerView()
                                .padding(.top, Layout.dividerPadding)
                        }
                        headingText(text)
                            .padding(.top, showDivider ? Layout.dividerPadding : top)
                    }

                case .unorderedList(let items):
                    VStack(alignment: .leading, spacing: 16) {
                        ForEach(items.indices, id: \.self) { i in
                            HStack(alignment: .firstTextBaseline, spacing: 8) {
                                Text("•")
                                    .font(.body.weight(.bold))
                                    .baselineOffset(2)
                                inlineText(items[i])
                                    .frame(maxWidth: .infinity, alignment: .leading)
                            }
                        }
                        .padding(.horizontal, 6)
                    }
                    .padding(.top, top)

                case .orderedList(let items):
                    VStack(alignment: .leading, spacing: 12) {
                        ForEach(items.indices, id: \.self) { i in
                            HStack(alignment: .firstTextBaseline, spacing: 10) {
                                Text("\(i + 1).")
                                    .font(.body.weight(.semibold))
                                inlineText(items[i])
                                    .frame(maxWidth: .infinity, alignment: .leading)
                            }
                        }
                    }
                    .padding(.top, top)

                case .paragraph(let text):
                    inlineText(text)
                        .padding(.top, top)

                case .quote(let text):
                    HStack(alignment: .top, spacing: 10) {
                        Rectangle()
                            .fill(Color.secondary.opacity(0.35))
                            .frame(width: 3)
                            .cornerRadius(1.5)
                        inlineText(text)
                    }
                    .padding(.top, top)

                case .rule:
                    dividerView()
                        .padding(.top, top)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private func dividerView() -> some View {
        Rectangle()
            .fill(Color.secondary.opacity(Layout.dividerOpacity))
            .frame(height: Layout.dividerHeight)
            .accessibilityHidden(true)
    }

    private func inlineText(_ text: String) -> some View {
        let transformed = applyInlineTypography(to: text)
        let rendered: Text
        if let attributed = try? AttributedString(
            markdown: transformed,
            options: .init(interpretedSyntax: .inlineOnlyPreservingWhitespace)
        ) {
            rendered = Text(attributed)
        } else {
            rendered = Text(transformed)
        }

        return rendered
            .font(.body)
            .lineSpacing(2)
            .fixedSize(horizontal: false, vertical: true)
            .layoutPriority(1)
    }

    private func headingText(_ text: String) -> some View {
        let rendered: Text
        if let attributed = try? AttributedString(
            markdown: text,
            options: .init(interpretedSyntax: .inlineOnlyPreservingWhitespace)
        ) {
            rendered = Text(attributed)
        } else {
            rendered = Text(text)
        }

        return rendered
            .font(.system(size: Layout.headingFontSize, weight: .semibold))
            .fixedSize(horizontal: false, vertical: true)
    }

    private func applyInlineTypography(to input: String) -> String {
        let stripped = stripWrappingEmphasis(input)
        return transformOutsideInlineCode(stripped) { segment in
            var s = segment
            s = s.replacingOccurrences(of: "->", with: " → ")
            s = s.replacingOccurrences(of: "<-", with: " ← ")
            s = s.replacingOccurrences(of: " --- ", with: " — ")
            s = s.replacingOccurrences(of: " -- ", with: " — ")
            s = s.replacingOccurrences(of: "→", with: " → ")
            s = s.replacingOccurrences(of: "←", with: " ← ")
            return collapseExtraSpaces(s)
        }
    }

    private func stripWrappingEmphasis(_ input: String) -> String {
        let trimmed = input.trimmingCharacters(in: .whitespaces)
        if trimmed.hasPrefix("*") && trimmed.hasSuffix("*") {
            let inner = String(trimmed.dropFirst().dropLast())
            if !inner.contains("*") {
                return inner
            }
        }
        if trimmed.hasPrefix("_") && trimmed.hasSuffix("_") {
            let inner = String(trimmed.dropFirst().dropLast())
            if !inner.contains("_") {
                return inner
            }
        }
        return input
    }

    private func topPadding(previous: Block?, current: Block, index: Int) -> CGFloat {
        if index == 0 { return 0 }
        if case .rule = current { return Layout.dividerPadding }
        if case .rule? = previous { return Layout.dividerPadding }

        if case .heading = previous, case .paragraph = current {
            if let first = firstHeadingIndex, index - 1 == first { return Layout.paragraphAfterFirstHeadingPadding }
            return Layout.defaultTopPadding
        }

        return Layout.defaultTopPadding
    }

    private func shouldInsertDivider(beforeHeadingAt index: Int, previous: Block?) -> Bool {
        if index == 0 { return false }
        if let first = firstHeadingIndex, index == first { return false }
        if case .rule? = previous { return false }
        return true
    }

    private func transformOutsideInlineCode(_ input: String, transform: (String) -> String) -> String {
        guard input.contains("`") else { return transform(input) }

        var result = ""
        result.reserveCapacity(input.count)

        var index = input.startIndex
        while index < input.endIndex {
            if input[index] == "`" {
                var runEnd = index
                while runEnd < input.endIndex, input[runEnd] == "`" {
                    runEnd = input.index(after: runEnd)
                }
                let runLength = input.distance(from: index, to: runEnd)
                let delimiter = String(repeating: "`", count: runLength)

                if let closingRange = input.range(of: delimiter, range: runEnd..<input.endIndex) {
                    let codeEnd = closingRange.upperBound
                    result.append(contentsOf: input[index..<codeEnd])
                    index = codeEnd
                } else {
                    result.append(contentsOf: transform(String(input[index...])))
                    break
                }
            } else {
                let nextTick = input[index...].firstIndex(of: "`") ?? input.endIndex
                result.append(contentsOf: transform(String(input[index..<nextTick])))
                index = nextTick
            }
        }

        return result
    }

    private func collapseExtraSpaces(_ input: String) -> String {
        var result = ""
        result.reserveCapacity(input.count)

        var previousWasSpace = false
        for ch in input {
            if ch == " " {
                if !previousWasSpace {
                    result.append(ch)
                    previousWasSpace = true
                }
            } else {
                result.append(ch)
                previousWasSpace = false
            }
        }

        return result
    }

    private static func parseBlocks(_ input: String) -> [Block] {
        let lines = input.split(omittingEmptySubsequences: false, whereSeparator: { $0.isNewline }).map { String($0) }
        var blocks: [Block] = []

        var paragraphBuffer: [String] = []
        var ulBuffer: [String] = []
        var olBuffer: [String] = []
        var quoteBuffer: [String] = []

        func flushParagraph() {
            if !paragraphBuffer.isEmpty {
                let text = paragraphBuffer.joined(separator: " ").trimmingCharacters(in: .whitespaces)
                blocks.append(.paragraph(text))
                paragraphBuffer.removeAll()
            }
        }
        func flushUL() {
            if !ulBuffer.isEmpty {
                blocks.append(.unorderedList(items: ulBuffer))
                ulBuffer.removeAll()
            }
        }
        func flushOL() {
            if !olBuffer.isEmpty {
                blocks.append(.orderedList(items: olBuffer))
                olBuffer.removeAll()
            }
        }
        func flushQuote() {
            if !quoteBuffer.isEmpty {
                let text = quoteBuffer.joined(separator: " ").trimmingCharacters(in: .whitespaces)
                blocks.append(.quote(text))
                quoteBuffer.removeAll()
            }
        }
        func flushAll() {
            flushParagraph()
            flushUL()
            flushOL()
            flushQuote()
        }

        for raw in lines {
            let line = raw.trimmingCharacters(in: CharacterSet.whitespaces)

            if line.isEmpty {
                flushAll()
                continue
            }

            if line == "---" || line == "***" || line == "___" {
                flushAll()
                blocks.append(.rule)
                continue
            }

            if line.hasPrefix("#") {
                let hashes = line.prefix { $0 == "#" }
                let after = line.dropFirst(hashes.count).trimmingCharacters(in: CharacterSet.whitespaces)
                flushAll()
                blocks.append(.heading(text: String(after)))
                continue
            }

            if let range = line.range(of: "^>\\s*", options: .regularExpression) {
                flushParagraph(); flushUL(); flushOL()
                let content = String(line[range.upperBound...]).trimmingCharacters(in: CharacterSet.whitespaces)
                quoteBuffer.append(content)
                continue
            }

            if let range = line.range(of: "^\\d+\\.\\s+", options: .regularExpression) {
                flushParagraph(); flushUL(); flushQuote()
                let item = String(line[range.upperBound...]).trimmingCharacters(in: CharacterSet.whitespaces)
                olBuffer.append(item)
                continue
            }

            if let range = line.range(of: "^[-*+]\\s+", options: .regularExpression) {
                flushParagraph(); flushOL(); flushQuote()
                let item = String(line[range.upperBound...]).trimmingCharacters(in: CharacterSet.whitespaces)
                ulBuffer.append(item)
                continue
            }

            flushQuote()
            flushUL()
            flushOL()
            paragraphBuffer.append(line)
        }

        flushAll()
        return blocks
    }
}