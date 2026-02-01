#include <metal_stdlib>
using namespace metal;

struct VertexOut {
    float4 position [[position]];
    float2 uv;
};

struct Uniforms {
    float2 resolution;
    float  time;
    float  level;      // 0..1
    float  isSpeaking; // 0 or 1
};

vertex VertexOut orbVertex(uint vid [[vertex_id]]) {
    // Full-screen triangle strip (4 verts)
    float2 pos;
    float2 uv;
    switch (vid) {
        case 0: pos = float2(-1.0, -1.0); uv = float2(0.0, 1.0); break;
        case 1: pos = float2( 1.0, -1.0); uv = float2(1.0, 1.0); break;
        case 2: pos = float2(-1.0,  1.0); uv = float2(0.0, 0.0); break;
        default: pos = float2( 1.0,  1.0); uv = float2(1.0, 0.0); break;
    }
    VertexOut o;
    o.position = float4(pos, 0.0, 1.0);
    o.uv = uv;
    return o;
}

static inline float hash21(float2 p) {
    // Simple stable hash
    p = fract(p * float2(123.34, 345.45));
    p += dot(p, p + 34.345);
    return fract(p.x * p.y);
}

static inline float noise(float2 p) {
    float2 i = floor(p);
    float2 f = fract(p);
    float a = hash21(i);
    float b = hash21(i + float2(1.0, 0.0));
    float c = hash21(i + float2(0.0, 1.0));
    float d = hash21(i + float2(1.0, 1.0));
    float2 u = f * f * (3.0 - 2.0 * f);
    return mix(mix(a, b, u.x), mix(c, d, u.x), u.y);
}

fragment float4 orbFragment(VertexOut in [[stage_in]],
                            constant Uniforms& u [[buffer(0)]]) {
    float t = u.time;
    float level = clamp(u.level, 0.0, 1.0);
    float speak = clamp(u.isSpeaking, 0.0, 1.0);

    float2 uv = in.uv;
    float2 p = (uv - 0.5) * 2.0;
    // Keep circle round even in non-square views
    float aspect = (u.resolution.x > 0.0 && u.resolution.y > 0.0) ? (u.resolution.x / u.resolution.y) : 1.0;
    p.x *= aspect;

    float r = length(p);
    float edge = smoothstep(1.0, 0.98, r);           // 1 inside, 0 outside
    float alpha = edge;

    // Soft outer glow that reacts to level while speaking.
    float glow = smoothstep(1.05, 0.70, r) * (0.18 + 0.55 * level) * (0.35 + 0.65 * speak);

    // Metallic base: dark core + bright rim + top-left specular.
    float3 baseDark = float3(0.05, 0.05, 0.08);
    float3 baseMid  = float3(0.16, 0.14, 0.22);
    float3 baseLight= float3(0.88, 0.88, 0.92);

    float rim = smoothstep(0.95, 0.60, r);
    float core = smoothstep(0.55, 0.00, r);

    // Specular highlight direction (top-left)
    float2 hlDir = normalize(float2(-0.65, -0.85));
    float hl = clamp(dot(normalize(p + 1e-4), hlDir) * 0.5 + 0.5, 0.0, 1.0);
    hl = pow(hl, 10.0) * (0.35 + 0.45 * level);

    // Animated “liquid metal” shimmer (noise warped by time + level)
    float2 q = p * (2.5 + 1.2 * level) + float2(t * 0.35, -t * 0.28);
    float n = noise(q) * 0.65 + noise(q * 2.0) * 0.35;
    float shimmer = (n - 0.5) * (0.20 + 0.55 * level) * (0.25 + 0.75 * speak);

    // Pulse ring driven by level (only when speaking)
    float ringCenter = 0.62 - 0.10 * level;
    float ringW = 0.06 + 0.04 * level;
    float ring = exp(-pow((r - ringCenter) / ringW, 2.0) * 3.0) * speak;
    ring *= (0.25 + 0.75 * level) * (0.6 + 0.4 * sin(t * 5.5 + n * 6.28) * 0.5 + 0.5);

    float3 col = mix(baseDark, baseMid, core);
    col = mix(col, baseLight, rim * 0.55);

    // Add subtle purple/blue hue shift with activity.
    float3 hue = float3(0.40, 0.22, 0.75);
    col = mix(col, hue, (0.08 + 0.22 * level) * speak);

    col += shimmer;
    col += baseLight * hl;
    col += hue * ring;
    col += hue * glow;

    // Inner “speaker cone” feel
    float inner = smoothstep(0.70, 0.25, r);
    col = mix(col, col * 0.78, inner * 0.55);

    // Premultiply-ish fade at edge
    col *= alpha;

    // Outside circle: fully transparent
    return float4(col, alpha);
}

