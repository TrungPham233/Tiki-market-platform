from pathlib import Path
import json


def test_pbip_files_exist():
    base = Path("E:/baitap/Project1/tiki-market-platform/dashboard")
    assert (base / "tiki_market_analytics.pbip").exists()
    assert (base / "tiki_market_analytics.Report").exists()
    assert (base / "tiki_market_analytics.SemanticModel").exists()


def test_pages_metadata_integrity():
    pages_json_path = Path("E:/baitap/Project1/tiki-market-platform/dashboard/tiki_market_analytics.Report/definition/pages/pages.json")
    assert pages_json_path.exists()

    with open(pages_json_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    assert "pageOrder" in meta
    assert len(meta["pageOrder"]) == 7
    assert meta["activePageName"] == meta["pageOrder"][0]


def test_all_pages_and_visuals():
    pages_dir = Path("E:/baitap/Project1/tiki-market-platform/dashboard/tiki_market_analytics.Report/definition/pages")
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
    tables_dir = Path("E:/baitap/Project1/tiki-market-platform/dashboard/tiki_market_analytics.SemanticModel/definition/tables")
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
    rel_path = Path("E:/baitap/Project1/tiki-market-platform/dashboard/tiki_market_analytics.SemanticModel/definition/relationships.tmdl")
    content = rel_path.read_text(encoding="utf-8")
    assert "rel_fact_product_to_dim_product" in content
    assert "rel_fact_product_to_dim_brand" in content
    assert "rel_fact_product_to_dim_seller" in content
    assert "rel_fact_product_to_dim_subcategory" in content
    assert "rel_fact_product_to_dim_date" in content
    assert "rel_fact_reviews_to_dim_product" in content
    assert "rel_fact_reviews_to_dim_date" in content
