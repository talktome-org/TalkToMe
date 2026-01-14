import Foundation

extension ChatStore {

    func canonicalRemoteURL(_ urlString: String) -> String {
        guard let url = URL(string: urlString) else { return urlString }
        guard let scheme = url.scheme, (scheme == "http" || scheme == "https") else { return urlString }
        guard let host = url.host else { return urlString }
        var out = "\(scheme)://\(host)"
        if let port = url.port { out += ":\(port)" }
        out += url.path
        return out
    }

    func decodeTalkToMeMeta(_ any: Any?) -> [String: Any]? {
        if let d = any as? [String: Any] { return d }
        if let s = any as? String, let sd = s.data(using: .utf8) {
            return (try? JSONSerialization.jsonObject(with: sd)) as? [String: Any]
        }
        return nil
    }

    func extractRemoteAttachments(messageId: String, content: String) -> [RemoteAttachment] {
        guard let data = content.data(using: .utf8) else { return [] }
        guard let objAny = try? JSONSerialization.jsonObject(with: data) else { return [] }
        guard let obj = objAny as? [String: Any] else { return [] }

        guard let meta = decodeTalkToMeMeta(obj["_talktome"]), (meta["type"] as? String) == "segments" else { return [] }
        guard let segs = meta["segments"] as? [Any] else { return [] }

        return segs
            .compactMap { $0 as? [String: Any] }
            .compactMap { seg in
                guard let type = seg["type"] as? String, (type == "image" || type == "file") else { return nil }
                guard let url = seg["url"] as? String, !url.isEmpty else { return nil }
                return RemoteAttachment(
                    messageId: messageId,
                    kind: type,
                    remoteURL: canonicalRemoteURL(url),
                    downloadURL: url,
                    contentType: seg["content_type"] as? String,
                    filename: seg["filename"] as? String
                )
            }
    }

    func rewriteSegmentURLs(content: String, replacements: [String: String]) -> String {
        guard !replacements.isEmpty else { return content }
        guard let data = content.data(using: .utf8) else { return content }
        guard var obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else { return content }
        guard var meta = decodeTalkToMeMeta(obj["_talktome"]) else { return content }
        guard (meta["type"] as? String) == "segments" else { return content }
        guard var segs = meta["segments"] as? [Any] else { return content }

        var changed = false
        segs = segs.map { segAny in
            guard var seg = segAny as? [String: Any] else { return segAny }
            guard let url = seg["url"] as? String else { return segAny }
            if let repl = replacements[canonicalRemoteURL(url)] {
                seg["url"] = repl
                changed = true
                return seg
            }
            return segAny
        }
        if !changed { return content }

        meta["segments"] = segs
        obj["_talktome"] = meta
        guard let out = try? JSONSerialization.data(withJSONObject: obj) else { return content }
        return String(data: out, encoding: .utf8) ?? content
    }
}