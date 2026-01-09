import SwiftUI

struct MarkdownRendererView: View {
    let markdown: String

    private let dividerTopSpacing: CGFloat = 30
    private let dividerBottomSpacing: CGFloat = 30

    private enum Block: Equatable {
        case heading(level: Int, text: String)
        case unorderedList(items: [String])
        case orderedList(items: [String])
        case paragraph(String)
        case rule
        case quote(String)
    }

    private struct BlockItem: Identifiable, Equatable {
        let id = UUID()
        let block: Block
    }

    var body: some View {
        let items = parse(markdown)
        let firstHeadingIndex = items.firstIndex { if case .heading = $0.block { return true } else { return false } }
        return VStack(alignment: .leading, spacing: 0) {
            ForEach(Array(items.enumerated()), id: \.element.id) { index, item in
                let prev: Block? = index > 0 ? items[index - 1].block : nil
                let top = topPadding(previous: prev, current: item.block, currentIndex: index, firstHeadingIndex: firstHeadingIndex)
                switch item.block {
                case .heading(_, let text):
                    let showDivider = shouldInsertDivider(beforeHeadingAt: index, previous: prev, firstHeadingIndex: firstHeadingIndex)
                    VStack(alignment: .leading, spacing: 0) {
                        if showDivider {
                            Divider()
                                .opacity(0.3)
                                .padding(.top, dividerTopSpacing)
                        }
                        Group {
                            if let attributed = try? AttributedString(
                                markdown: text,

                                options: .init(interpretedSyntax: .inlineOnlyPreservingWhitespace)
                            ) {
                                Text(attributed)
                            } else {
                                Text(text)
                            }
                        }
                        .font(.system(size: 21, weight: .semibold))
                        .padding(.top, showDivider ? dividerBottomSpacing : top)
                    }
                case .unorderedList(let items):
                    VStack(alignment: .leading, spacing: 16) {
                        ForEach(Array(items.enumerated()), id: \.0) { _, raw in
                            HStack(alignment: .firstTextBaseline, spacing: 8) {
                                Text("•")
                                    .font(.system(size: 13, weight: .bold))
                                    .baselineOffset(2)
                                inlineText(raw)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                            }
                        }
                        .padding(.horizontal, 6)
                    }
                    .padding(.top, top)
                case .orderedList(let items):
                    VStack(alignment: .leading, spacing: 12) {
                        ForEach(Array(items.enumerated()), id: \.0) { idx, raw in
                            HStack(alignment: .firstTextBaseline, spacing: 10) {
                                Text("\(idx + 1).")
                                    .font(.system(size: 17, weight: .semibold))
                                inlineText(raw)
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
                    Divider()
                        .opacity(0.3)
                        .padding(.top, top)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private func inlineText(_ text: String) -> some View {
        let transformed = applyInlineTypography(to: text)
        if let attributed = try? AttributedString(markdown: transformed, options: .init(interpretedSyntax: .inlineOnlyPreservingWhitespace)) {
            return Text(attributed)
                .font(.system(size: 17, weight: .regular))
                .lineSpacing(2)
                .fixedSize(horizontal: false, vertical: true)
                .layoutPriority(1)
        } else {
            return Text(transformed)
                .font(.system(size: 17, weight: .regular))
                .lineSpacing(2)
                .fixedSize(horizontal: false, vertical: true)
                .layoutPriority(1)
        }
    }

    private func applyInlineTypography(to input: String) -> String {
        var s = input
        s = stripFullSentenceItalics(s)
        s = s.replacingOccurrences(of: "->", with: " → ")
        s = s.replacingOccurrences(of: "<-", with: " ← ")
        s = s.replacingOccurrences(of: " --- ", with: " — ")
        s = s.replacingOccurrences(of: " -- ", with: " — ")
        s = s.replacingOccurrences(of: " - ", with: " — ")
        s = s.replacingOccurrences(of: "—", with: " — ")
        s = s.replacingOccurrences(of: "→", with: " → ")
        s = s.replacingOccurrences(of: "←", with: " ← ")
        while s.contains("  ") { s = s.replacingOccurrences(of: "  ", with: " ") }
        return s
    }

    private func stripFullSentenceItalics(_ input: String) -> String {
        let trimmed = input.trimmingCharacters(in: .whitespaces)
        if trimmed.hasPrefix("*") && trimmed.hasSuffix("*") {
            let inner = String(trimmed.dropFirst().dropLast())
            if !inner.contains("*") && !inner.contains("**") {
                return inner
            }
        }
        if trimmed.hasPrefix("_") && trimmed.hasSuffix("_") {
            let inner = String(trimmed.dropFirst().dropLast())
            if !inner.contains("_") && !inner.contains("__") {
                return inner
            }
        }
        return input
    }

    private func topPadding(previous: Block?, current: Block, currentIndex: Int, firstHeadingIndex: Int?) -> CGFloat {
        if currentIndex == 0 { return 0 }
        if case .rule = current { return dividerTopSpacing }
        if case .rule? = previous { return dividerBottomSpacing }

        if case .heading = previous, case .paragraph = current {
            if let first = firstHeadingIndex, currentIndex - 1 == first { return 22 }
            return 30
        }

        return 30
    }

    private func shouldInsertDivider(beforeHeadingAt index: Int, previous: Block?, firstHeadingIndex: Int?) -> Bool {
        if index == 0 { return false }
        if let first = firstHeadingIndex, index == first { return false }
        if case .rule? = previous { return false }
        return true
    }

    private func parse(_ input: String) -> [BlockItem] {
        let lines = input.split(omittingEmptySubsequences: false, whereSeparator: { $0.isNewline }).map { String($0) }
        var items: [BlockItem] = []

        var paragraphBuffer: [String] = []
        var ulBuffer: [String] = []
        var olBuffer: [String] = []
        var quoteBuffer: [String] = []

        func flushParagraph() {
            if !paragraphBuffer.isEmpty {
                let text = paragraphBuffer.joined(separator: " ").trimmingCharacters(in: .whitespaces)
                items.append(BlockItem(block: .paragraph(text)))
                paragraphBuffer.removeAll()
            }
        }
        func flushUL() {
            if !ulBuffer.isEmpty {
                if ulBuffer.count == 1,
                   let last = items.last,
                   case .heading = last.block,
                   !ulBuffer[0].trimmingCharacters(in: .whitespaces).hasSuffix("?") {
                    items.append(BlockItem(block: .paragraph(ulBuffer[0])))
                } else {
                    items.append(BlockItem(block: .unorderedList(items: ulBuffer)))
                }
                ulBuffer.removeAll()
            }
        }
        func flushOL() {
            if !olBuffer.isEmpty {
                items.append(BlockItem(block: .orderedList(items: olBuffer)))
                olBuffer.removeAll()
            }
        }
        func flushQuote() {
            if !quoteBuffer.isEmpty {
                let text = quoteBuffer.joined(separator: " ").trimmingCharacters(in: .whitespaces)
                items.append(BlockItem(block: .quote(text)))
                quoteBuffer.removeAll()
            }
        }

        var idx = 0
        while idx < lines.count {
            let raw = lines[idx]
            let line = raw.trimmingCharacters(in: CharacterSet.whitespaces)

            if line.isEmpty {
                flushParagraph(); flushUL(); flushQuote()
                idx += 1
                continue
            }

            let isOrdered = line.range(of: "^\\d+\\.\\s+", options: .regularExpression) != nil
            if !olBuffer.isEmpty && !isOrdered {
                flushOL()
            }

            if line == "---" || line == "***" || line == "___" {
                flushParagraph(); flushUL(); flushOL(); flushQuote()
                items.append(BlockItem(block: .rule))
                idx += 1
                continue
            }

            if line.hasPrefix("#") {
                let hashes = line.prefix { $0 == "#" }
                let after = line.dropFirst(hashes.count).trimmingCharacters(in: CharacterSet.whitespaces)
                let level = min(max(hashes.count, 1), 3)
                flushParagraph(); flushUL(); flushOL(); flushQuote()
                items.append(BlockItem(block: .heading(level: level, text: String(after))))
                idx += 1
                continue
            }

            if let range = line.range(of: "^>\\s+", options: .regularExpression) {
                flushParagraph(); flushUL(); flushOL()
                let content = String(line[range.upperBound...]).trimmingCharacters(in: CharacterSet.whitespaces)
                quoteBuffer.append(content)
                idx += 1
                continue
            }

            if let range = line.range(of: "^\\d+\\.\\s+", options: .regularExpression) {
                flushParagraph(); flushUL(); flushQuote()
                let item = String(line[range.upperBound...]).trimmingCharacters(in: CharacterSet.whitespaces)
                olBuffer.append(item)
                idx += 1
                continue
            }

            if let range = line.range(of: "^[-*+]\\s+", options: .regularExpression) {
                flushParagraph(); flushOL(); flushQuote()
                let item = String(line[range.upperBound...]).trimmingCharacters(in: CharacterSet.whitespaces)
                ulBuffer.append(item)
                idx += 1
                continue
            }

            if !quoteBuffer.isEmpty { flushQuote() }
            paragraphBuffer.append(line)
            idx += 1
        }

        flushParagraph(); flushUL(); flushOL(); flushQuote()
        return items
    }
}