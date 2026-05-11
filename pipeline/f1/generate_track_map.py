"""
Generate accurate F1 circuit SVG maps from FastF1 telemetry data.
Outputs SVG path strings for embedding in the frontend.
"""
import sys
sys.path.insert(0, '/app')

import json
import numpy as np
import fastf1
import warnings
warnings.filterwarnings("ignore")

fastf1.Cache.enable_cache("/tmp/f1cache")


def normalize_coords(points, width=500, height=320, padding=30):
    """Scale and center GPS coordinates to fit SVG viewBox."""
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]

    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)

    scale_x = (width - 2 * padding) / (max_x - min_x)
    scale_y = (height - 2 * padding) / (max_y - min_y)
    scale = min(scale_x, scale_y)

    cx = (width - (max_x - min_x) * scale) / 2
    cy = (height - (max_y - min_y) * scale) / 2

    normalized = []
    for x, y in points:
        nx = (x - min_x) * scale + cx
        # Flip Y axis (SVG Y increases downward)
        ny = height - ((y - min_y) * scale + cy)
        normalized.append((round(nx, 1), round(ny, 1)))

    return normalized


def build_svg_path(points):
    """Convert point list to SVG path string."""
    if not points:
        return ""
    path = f"M {points[0][0]},{points[0][1]}"
    for x, y in points[1:]:
        path += f" L {x},{y}"
    path += " Z"
    return path


def get_sector_split_indices(coords_df, total_dist, s1_pct=0.38, s2_pct=0.72):
    """Find row indices where sector boundaries fall."""
    s1_dist = total_dist * s1_pct
    s2_dist = total_dist * s2_pct

    s1_idx = int((coords_df['Distance'] - s1_dist).abs().idxmin())
    s2_idx = int((coords_df['Distance'] - s2_dist).abs().idxmin())

    return s1_idx, s2_idx


def generate_circuit_map(year, race_name, session_type='R',
                         width=500, height=320, padding=30):
    """Generate SVG track map with per-sector paths."""
    session = fastf1.get_session(year, race_name, session_type)
    session.load(telemetry=True, laps=True, weather=False)

    fastest = session.laps.pick_fastest()
    tel = fastest.get_telemetry()
    coords = tel[['X', 'Y', 'Distance']].dropna().reset_index(drop=True)

    points = [(float(r.X), float(r.Y)) for _, r in coords.iterrows()]
    total_dist = float(coords['Distance'].max())

    norm_points = normalize_coords(points, width, height, padding)

    s1_idx, s2_idx = get_sector_split_indices(coords, total_dist)

    sector1_pts = norm_points[:s1_idx]
    sector2_pts = norm_points[s1_idx:s2_idx]
    sector3_pts = norm_points[s2_idx:]

    sf_x, sf_y = norm_points[0]

    return {
        "race": race_name,
        "year": year,
        "total_points": len(points),
        "sector1_path": build_svg_path(sector1_pts),
        "sector2_path": build_svg_path(sector2_pts),
        "sector3_path": build_svg_path(sector3_pts),
        "full_path": build_svg_path(norm_points),
        "sf_x": sf_x,
        "sf_y": sf_y,
        "s1_boundary": list(norm_points[s1_idx]),
        "s2_boundary": list(norm_points[s2_idx]),
        "viewBox": f"0 0 {width} {height}",
        "width": width,
        "height": height,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--race", type=str, default="Miami")
    parser.add_argument("--output", type=str, default="/tmp/track_map.json")
    args = parser.parse_args()

    print(f"Generating track map for {args.year} {args.race}...")
    result = generate_circuit_map(args.year, args.race)

    with open(args.output, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"Saved to {args.output}")
    print(f"Total points: {result['total_points']}")
    print(f"S/F position: {result['sf_x']}, {result['sf_y']}")
    print(f"S1 boundary:  {result['s1_boundary']}")
    print(f"S2 boundary:  {result['s2_boundary']}")
