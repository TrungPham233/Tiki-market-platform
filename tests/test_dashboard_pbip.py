from pathlib import Path
import json

ROOT_DIR = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT_DIR / "dashboard"
REPORT_DIR = BASE_DIR / "tiki_market_analytics.Report"
SEMANTIC_DIR = BASE_DIR / "tiki_market_analytics.SemanticModel"


def test_pbip_files_exist():
    assert (BASE_DIR / "tiki_market_analytics.pbip").exists()
    assert REPORT_DIR.exists()
    assert SEMANTIC_DIR.exists()


def test_pages_metadata_integrity():
    pages_json_path = REPORT_DIR / "definition" / "pages" / "pages.json"
    assert pages_json_path.exists()

    with open(pages_json_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    assert "pageOrder" in meta
    assert len(meta["pageOrder"]) == 7
    assert meta["activePageName"] == meta["pageOrder"][0]


def test_all_pages_and_visuals():
    pages_dir = REPORT_DIR / "definition" / "pages"
    with open(pages_dir / "pages.json", "r", encoding="utf-8") as f:
        meta = json.load(f)

    total_visuals = 0
    for page_id in meta["pageOrder"]:
        p_dir = pages_dir / page_id
        assert p_dir.exists(), f"Page folder {page_id} missing"

        page_file = p_dir / "page.json"
        assert page_file.exists(), f"page.json in {page_id} missing"
        with open(page_file, "r", encoding="utf-8") as pf:
            pdata = json.load(pf)
            assert pdata["width"] == 1920
            assert pdata["height"] == 1080
            assert len(pdata["displayName"]) > 0

        vis_dir = p_dir / "visuals"
        assert vis_dir.exists(), f"visuals dir in {page_id} missing"
        visual_dirs = list(vis_dir.iterdir())
        assert len(visual_dirs) >= 4, f"Page {page_id} should have at least 4 visuals"

        for vd in visual_dirs:
            vf = vd / "visual.json"
            assert vf.exists(), f"Missing visual.json in {vd}"
            with open(vf, "r", encoding="utf-8") as vfp:
                vdata = json.load(vfp)
                assert "visual" in vdata
                assert "position" in vdata
                assert vdata["position"]["width"] > 0
                assert vdata["position"]["height"] > 0
                assert "visualType" in vdata["visual"]
                total_visuals += 1

    assert total_visuals >= 40, f"Expected at least 40 visuals across 7 pages, found {total_visuals}"


def test_semantic_model_tmdl():
    tables_dir = SEMANTIC_DIR / "definition" / "tables"
    fact_tmdl = (tables_dir / "fact_product_snapshot.tmdl").read_text(encoding="utf-8")
    assert "measure 'Total Estimated Revenue'" in fact_tmdl
    assert "measure 'Total Volume Sold'" in fact_tmdl
    assert "measure 'Average Rating'" in fact_tmdl
    assert "measure 'CR5 Index %'" in fact_tmdl

    reviews_tmdl = (tables_dir / "fact_reviews.tmdl").read_text(encoding="utf-8")
    assert "measure 'Total Reviews'" in reviews_tmdl
    assert "measure 'Positive Review Ratio %'" in reviews_tmdl
    assert "measure 'AfterSales Complaint %'" in reviews_tmdl
    assert "measure 'Hardware Defect %'" in reviews_tmdl


def test_relationships_star_schema():
    rel_path = SEMANTIC_DIR / "definition" / "relationships.tmdl"
    content = rel_path.read_text(encoding="utf-8")
    assert "rel_fact_product_to_dim_product" in content
    assert "rel_fact_product_to_dim_brand" in content
    assert "rel_fact_product_to_dim_seller" in content
    assert "rel_fact_product_to_dim_subcategory" in content
    assert "rel_fact_product_to_dim_date" in content
    assert "rel_fact_reviews_to_dim_product" in content
    assert "rel_fact_reviews_to_dim_date" in content


def test_visual_data_roles_and_theme():
    theme_file = REPORT_DIR / "StaticResources" / "RegisteredResources" / "TikiMarket_Theme.json"
    assert theme_file.exists(), "TikiMarket_Theme.json must exist"

    report_json = json.loads((REPORT_DIR / "definition" / "report.json").read_text(encoding="utf-8"))
    assert "customTheme" in report_json["themeCollection"]
    assert report_json["themeCollection"]["customTheme"]["name"] == "TikiMarket_Theme"

    # Verify visual roles across all pages
    pages_dir = REPORT_DIR / "definition" / "pages"
    for page_folder in pages_dir.glob("*/visuals/*"):
        vis_json = page_folder / "visual.json"
        if not vis_json.exists():
            continue
        vdata = json.loads(vis_json.read_text(encoding="utf-8"))
        vtype = vdata["visual"]["visualType"]
        qs = vdata["visual"].get("query", {}).get("queryState", {})

        if vtype == "treemap":
            assert "Group" in qs, f"Treemap {page_folder.name} must use 'Group' data role"
            assert "Values" in qs
        elif vtype == "scatterChart":
            assert "Category" in qs, f"Scatter {page_folder.name} must use 'Category' data role"
            assert "X" in qs and "Y" in qs
        elif vtype == "tableEx":
            assert "Values" in qs, f"Table {page_folder.name} must use 'Values' data role, not 'Rows'"
        elif vtype == "card":
            assert "Values" in qs
            cat_labels = vdata["visual"].get("objects", {}).get("categoryLabels", [{}])[0]
            show_val = cat_labels.get("properties", {}).get("show", {}).get("expr", {}).get("Literal", {}).get("Value")
            assert show_val == "false", f"Card {page_folder.name} must have categoryLabels show=false"
