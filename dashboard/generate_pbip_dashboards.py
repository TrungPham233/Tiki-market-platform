"""
Script to configure TMDL measures, Star Schema relationships,
and design the 7 analytical dashboard pages in the Power BI Project (PBIP).
"""

from pathlib import Path
import json
import re

BASE_DIR = Path("E:/baitap/Project1/tiki-market-platform/dashboard")
SEMANTIC_DIR = BASE_DIR / "tiki_market_analytics.SemanticModel" / "definition"
REPORT_DIR = BASE_DIR / "tiki_market_analytics.Report" / "definition"

SCHEMA_VISUAL = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.1.0/schema.json"
SCHEMA_PAGE = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"
SCHEMA_PAGES = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json"


def update_fact_product_snapshot():
    tmdl_path = SEMANTIC_DIR / "tables" / "fact_product_snapshot.tmdl"
    content = tmdl_path.read_text(encoding="utf-8")

    measures = """
\tmeasure 'Total Estimated Revenue' = SUMX(fact_product_snapshot, fact_product_snapshot[price] * fact_product_snapshot[quantity_sold])
\t\tformatString: #,##0₫
\t\tlineageTag: 71a8fb12-32a1-4354-a951-8743c4a20001

\tmeasure 'Total Volume Sold' = SUM(fact_product_snapshot[quantity_sold])
\t\tformatString: #,##0
\t\tlineageTag: 71a8fb12-32a1-4354-a951-8743c4a20002

\tmeasure 'Average Rating' = AVERAGE(fact_product_snapshot[rating_average])
\t\tformatString: 0.00
\t\tlineageTag: 71a8fb12-32a1-4354-a951-8743c4a20003

\tmeasure 'Total Active SKUs' = DISTINCTCOUNT(fact_product_snapshot[product_id])
\t\tformatString: #,##0
\t\tlineageTag: 71a8fb12-32a1-4354-a951-8743c4a20004

\tmeasure 'Average Discount Rate %' = AVERAGE(fact_product_snapshot[discount_rate_calc])
\t\tformatString: 0.0%
\t\tlineageTag: 71a8fb12-32a1-4354-a951-8743c4a20005

\tmeasure 'CR5 Index %' = ```
\t\t\tVAR Top5Revenue = 
\t\t\t    CALCULATE(
\t\t\t        [Total Estimated Revenue],
\t\t\t        TOPN(5, ALL(dim_brand[brand_name]), [Total Estimated Revenue], DESC)
\t\t\t    )
\t\t\tVAR TotalRevenueAll = 
\t\t\t    CALCULATE([Total Estimated Revenue], ALL(dim_brand))
\t\t\tRETURN
\t\t\t    DIVIDE(Top5Revenue, TotalRevenueAll, 0)
\t\t\t```
\t\tformatString: 0.0%
\t\tlineageTag: 71a8fb12-32a1-4354-a951-8743c4a20006
"""
    # Insert right after the table declaration and lineageTag
    if "measure 'Total Estimated Revenue'" not in content:
        target = "table fact_product_snapshot\n\tlineageTag: 5e3965a2-75be-4534-a815-b4c69496b368"
        replacement = target + "\n" + measures
        content = content.replace(target, replacement)
        tmdl_path.write_text(content, encoding="utf-8")
        print("Updated fact_product_snapshot.tmdl with DAX measures")
    else:
        print("fact_product_snapshot.tmdl already has measures")


def update_fact_reviews():
    tmdl_path = SEMANTIC_DIR / "tables" / "fact_reviews.tmdl"
    content = tmdl_path.read_text(encoding="utf-8")

    measures = """
\tmeasure 'Total Reviews' = COUNTROWS(fact_reviews)
\t\tformatString: #,##0
\t\tlineageTag: 82b9fc23-43b2-4896-b062-9854d5b30001

\tmeasure 'Positive Review Ratio %' = ```
\t\t\tVAR PositiveCount = 
\t\t\t    CALCULATE(COUNTROWS(fact_reviews), fact_reviews[sentiment_label] = "positive")
\t\t\tVAR TotalReviews = 
\t\t\t    COUNTROWS(fact_reviews)
\t\t\tRETURN
\t\t\t    DIVIDE(PositiveCount, TotalReviews, 0)
\t\t\t```
\t\tformatString: 0.0%
\t\tlineageTag: 82b9fc23-43b2-4896-b062-9854d5b30002

\tmeasure 'Negative Review Ratio %' = ```
\t\t\tVAR NegCount = 
\t\t\t    CALCULATE(COUNTROWS(fact_reviews), fact_reviews[sentiment_label] = "negative")
\t\t\tVAR TotalReviews = 
\t\t\t    COUNTROWS(fact_reviews)
\t\t\tRETURN
\t\t\t    DIVIDE(NegCount, TotalReviews, 0)
\t\t\t```
\t\tformatString: 0.0%
\t\tlineageTag: 82b9fc23-43b2-4896-b062-9854d5b30003

\tmeasure 'AfterSales Complaint %' = ```
\t\t\tVAR AfterSalesCount = 
\t\t\t    CALCULATE(
\t\t\t        COUNTROWS(fact_reviews),
\t\t\t        fact_reviews[is_after_sales_complaint] = TRUE(),
\t\t\t        fact_reviews[sentiment_label] = "negative"
\t\t\t    )
\t\t\tVAR TotalNegative = 
\t\t\t    CALCULATE(COUNTROWS(fact_reviews), fact_reviews[sentiment_label] = "negative")
\t\t\tRETURN
\t\t\t    DIVIDE(AfterSalesCount, TotalNegative, 0)
\t\t\t```
\t\tformatString: 0.0%
\t\tlineageTag: 82b9fc23-43b2-4896-b062-9854d5b30004

\tmeasure 'Hardware Defect %' = ```
\t\t\tVAR DefectCount = 
\t\t\t    CALCULATE(
\t\t\t        COUNTROWS(fact_reviews),
\t\t\t        fact_reviews[is_defect_complaint] = TRUE(),
\t\t\t        fact_reviews[sentiment_label] = "negative"
\t\t\t    )
\t\t\tVAR TotalNegative = 
\t\t\t    CALCULATE(COUNTROWS(fact_reviews), fact_reviews[sentiment_label] = "negative")
\t\t\tRETURN
\t\t\t    DIVIDE(DefectCount, TotalNegative, 0)
\t\t\t```
\t\tformatString: 0.0%
\t\tlineageTag: 82b9fc23-43b2-4896-b062-9854d5b30005

\tmeasure 'Reviews By Seller' = ```
\t\t\tCALCULATE(
\t\t\t    COUNTROWS(fact_reviews),
\t\t\t    TREATAS(
\t\t\t        VALUES(fact_product_snapshot[product_sk]),
\t\t\t        fact_reviews[product_sk]
\t\t\t    )
\t\t\t)
\t\t\t```
\t\tformatString: #,##0
\t\tlineageTag: 82b9fc23-43b2-4896-b062-9854d5b30006
"""
    if "measure 'Total Reviews'" not in content:
        target = "table fact_reviews\n\tlineageTag: d7cc88e5-2901-4ea3-ad9f-dae2e7d8df3f"
        replacement = target + "\n" + measures
        content = content.replace(target, replacement)
        tmdl_path.write_text(content, encoding="utf-8")
        print("Updated fact_reviews.tmdl with DAX measures")
    else:
        print("fact_reviews.tmdl already has measures")


def update_agg_tables():
    p1 = SEMANTIC_DIR / "tables" / "agg_discount_saturation.tmdl"
    c1 = p1.read_text(encoding="utf-8")
    if "measure 'Avg Units Sold'" not in c1:
        m1 = """
\tmeasure 'Avg Units Sold' = AVERAGE(agg_discount_saturation[avg_quantity_sold])
\t\tformatString: #,##0.0
\t\tlineageTag: 93a8fb12-32a1-4354-a951-8743c4a20007

\tmeasure 'Discount SKU Count' = SUM(agg_discount_saturation[sku_count])
\t\tformatString: #,##0
\t\tlineageTag: 93a8fb12-32a1-4354-a951-8743c4a20008
"""
        target = "table agg_discount_saturation\n\tlineageTag: 48c84218-7394-4db6-9528-8761e54a102c"
        c1 = c1.replace(target, target + "\n" + m1)
        p1.write_text(c1, encoding="utf-8")
        print("Updated agg_discount_saturation.tmdl")

    p2 = SEMANTIC_DIR / "tables" / "agg_voc_complaint_causes.tmdl"
    c2 = p2.read_text(encoding="utf-8")
    if "measure 'Total Complaint Count'" not in c2:
        m2 = """
\tmeasure 'Total Complaint Count' = SUM(agg_voc_complaint_causes[complaint_count])
\t\tformatString: #,##0
\t\tlineageTag: a4a8fb12-32a1-4354-a951-8743c4a20009

\tmeasure 'Complaint % Share' = AVERAGE(agg_voc_complaint_causes[percentage_of_negative])
\t\tformatString: 0.0%
\t\tlineageTag: a4a8fb12-32a1-4354-a951-8743c4a20010
"""
        target = "table agg_voc_complaint_causes\n\tlineageTag: a098afdc-64e8-4aae-9dd3-89fb077c6810"
        c2 = c2.replace(target, target + "\n" + m2)
        p2.write_text(c2, encoding="utf-8")
        print("Updated agg_voc_complaint_causes.tmdl")


def update_relationships():
    tmdl_path = SEMANTIC_DIR / "relationships.tmdl"
    clean_relationships = """relationship 1ba175a7-1369-4d5c-b17f-1181d4869340
\tjoinOnDateBehavior: datePartOnly
\tfromColumn: alerts.triggered_at
\ttoColumn: LocalDateTable_6d4fb0ab-8bee-4ffc-b7a7-8f9e402fc14d.Date

relationship f86cd2fd-5c06-4866-b3f4-29e594c28095
\tjoinOnDateBehavior: datePartOnly
\tfromColumn: dim_product.valid_from
\ttoColumn: LocalDateTable_d8734ea3-55e0-496c-8ed5-b85992fa09f8.Date

relationship abea4c30-7b78-4132-acc9-bdd14c0aa153
\tjoinOnDateBehavior: datePartOnly
\tfromColumn: dim_product.valid_to
\ttoColumn: LocalDateTable_9ba340d7-d7d0-4d5a-882d-6988c0748b12.Date

relationship a714cf22-620d-4e7e-ad60-128e7655557e
\tjoinOnDateBehavior: datePartOnly
\tfromColumn: dim_date.full_date
\ttoColumn: LocalDateTable_ea42d29c-00d0-4b1e-801d-8eb4174a121f.Date

// Star Schema Core Fact Relationships (All ACTIVE)
relationship rel_fact_product_to_dim_product
\tfromColumn: fact_product_snapshot.product_id
\ttoColumn: dim_product.product_id

relationship rel_fact_product_to_dim_brand
\tfromColumn: fact_product_snapshot.brand_id
\ttoColumn: dim_brand.brand_id

relationship rel_fact_product_to_dim_seller
\tfromColumn: fact_product_snapshot.seller_id
\ttoColumn: dim_seller.seller_id

relationship rel_fact_product_to_dim_subcategory
\tfromColumn: fact_product_snapshot.subcategory_id
\ttoColumn: dim_subcategory.subcategory_id

relationship rel_fact_product_to_dim_date
\tfromColumn: fact_product_snapshot.date_key
\ttoColumn: dim_date.date_key

// Fact Reviews to Product and Date Dimensions (ACTIVE)
relationship rel_fact_reviews_to_dim_product
\tfromColumn: fact_reviews.product_id
\ttoColumn: dim_product.product_id

relationship rel_fact_reviews_to_dim_date
\tfromColumn: fact_reviews.date_key
\ttoColumn: dim_date.date_key

// Inactive secondary paths to avoid circular / snowflake loops
relationship rel_dim_product_to_dim_brand_inactive
\tisActive: false
\tfromColumn: dim_product.brand_id
\ttoColumn: dim_brand.brand_id

relationship rel_dim_product_to_dim_seller_inactive
\tisActive: false
\tfromColumn: dim_product.seller_id
\ttoColumn: dim_seller.seller_id

relationship rel_dim_product_to_dim_subcategory_inactive
\tisActive: false
\tfromColumn: dim_product.subcategory_id
\ttoColumn: dim_subcategory.subcategory_id

relationship rel_fact_reviews_product_sk_inactive
\tisActive: false
\tfromColumn: fact_reviews.product_sk
\ttoColumn: fact_product_snapshot.product_sk
"""
    tmdl_path.write_text(clean_relationships, encoding="utf-8")
    print("Updated relationships.tmdl with pure Star Schema")


# Visual builders
def make_textbox(name, x, y, w, h, text):
    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": 100, "height": h, "width": w, "tabOrder": 100},
        "visual": {
            "visualType": "textbox",
            "objects": {
                "general": [
                    {
                        "properties": {
                            "paragraphs": [
                                {
                                    "textRuns": [
                                        {
                                            "value": text
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def make_card(name, x, y, w, h, entity, measure_name, title, tab_order=1000):
    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": tab_order, "height": h, "width": w, "tabOrder": tab_order},
        "visual": {
            "visualType": "card",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": entity}},
                                        "Property": measure_name
                                    }
                                },
                                "queryRef": f"{entity}.{measure_name}"
                            }
                        ]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def make_bar_chart(name, x, y, w, h, cat_entity, cat_col, val_entity, val_measure, title, tab_order=2000):
    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": tab_order, "height": h, "width": w, "tabOrder": tab_order},
        "visual": {
            "visualType": "clusteredBarChart",
            "query": {
                "queryState": {
                    "Category": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Entity": cat_entity}},
                                        "Property": cat_col
                                    }
                                },
                                "queryRef": f"{cat_entity}.{cat_col}"
                            }
                        ]
                    },
                    "Y": {
                        "projections": [
                            {
                                "field": {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": val_entity}},
                                        "Property": val_measure
                                    }
                                },
                                "queryRef": f"{val_entity}.{val_measure}"
                            }
                        ]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def make_column_chart(name, x, y, w, h, cat_entity, cat_col, val_entity, val_measure, title, tab_order=2000):
    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": tab_order, "height": h, "width": w, "tabOrder": tab_order},
        "visual": {
            "visualType": "clusteredColumnChart",
            "query": {
                "queryState": {
                    "Category": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Entity": cat_entity}},
                                        "Property": cat_col
                                    }
                                },
                                "queryRef": f"{cat_entity}.{cat_col}"
                            }
                        ]
                    },
                    "Y": {
                        "projections": [
                            {
                                "field": {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": val_entity}},
                                        "Property": val_measure
                                    }
                                },
                                "queryRef": f"{val_entity}.{val_measure}"
                            }
                        ]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def make_treemap(name, x, y, w, h, cat_entity, cat_col, val_entity, val_measure, title, tab_order=2000):
    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": tab_order, "height": h, "width": w, "tabOrder": tab_order},
        "visual": {
            "visualType": "treemap",
            "query": {
                "queryState": {
                    "Category": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Entity": cat_entity}},
                                        "Property": cat_col
                                    }
                                },
                                "queryRef": f"{cat_entity}.{cat_col}"
                            }
                        ]
                    },
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": val_entity}},
                                        "Property": val_measure
                                    }
                                },
                                "queryRef": f"{val_entity}.{val_measure}"
                            }
                        ]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def make_donut_chart(name, x, y, w, h, cat_entity, cat_col, val_entity, val_measure, title, tab_order=2000):
    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": tab_order, "height": h, "width": w, "tabOrder": tab_order},
        "visual": {
            "visualType": "donutChart",
            "query": {
                "queryState": {
                    "Category": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Entity": cat_entity}},
                                        "Property": cat_col
                                    }
                                },
                                "queryRef": f"{cat_entity}.{cat_col}"
                            }
                        ]
                    },
                    "Y": {
                        "projections": [
                            {
                                "field": {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": val_entity}},
                                        "Property": val_measure
                                    }
                                },
                                "queryRef": f"{val_entity}.{val_measure}"
                            }
                        ]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def make_scatter_chart(name, x, y, w, h, x_entity, x_prop, y_entity, y_prop, det_entity, det_col, size_entity, size_measure, title, is_x_measure=False, is_y_measure=False, tab_order=2000):
    x_field = {
        "Measure" if is_x_measure else "Column": {
            "Expression": {"SourceRef": {"Entity": x_entity}},
            "Property": x_prop
        }
    }
    y_field = {
        "Measure" if is_y_measure else "Column": {
            "Expression": {"SourceRef": {"Entity": y_entity}},
            "Property": y_prop
        }
    }

    query_state = {
        "X": {
            "projections": [
                {
                    "field": x_field,
                    "queryRef": f"{x_entity}.{x_prop}"
                }
            ]
        },
        "Y": {
            "projections": [
                {
                    "field": y_field,
                    "queryRef": f"{y_entity}.{y_prop}"
                }
            ]
        },
        "Details": {
            "projections": [
                {
                    "field": {
                        "Column": {
                            "Expression": {"SourceRef": {"Entity": det_entity}},
                            "Property": det_col
                        }
                    },
                    "queryRef": f"{det_entity}.{det_col}"
                }
            ]
        }
    }

    if size_entity and size_measure:
        query_state["Size"] = {
            "projections": [
                {
                    "field": {
                        "Measure": {
                            "Expression": {"SourceRef": {"Entity": size_entity}},
                            "Property": size_measure
                        }
                    },
                    "queryRef": f"{size_entity}.{size_measure}"
                }
            ]
        }

    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": tab_order, "height": h, "width": w, "tabOrder": tab_order},
        "visual": {
            "visualType": "scatterChart",
            "query": {
                "queryState": query_state
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def make_table(name, x, y, w, h, items, title, tab_order=3000):
    # items: list of (entity, prop, is_measure)
    projections = []
    for entity, prop, is_measure in items:
        field = {
            "Measure" if is_measure else "Column": {
                "Expression": {"SourceRef": {"Entity": entity}},
                "Property": prop
            }
        }
        projections.append({
            "field": field,
            "queryRef": f"{entity}.{prop}"
        })

    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": tab_order, "height": h, "width": w, "tabOrder": tab_order},
        "visual": {
            "visualType": "tableEx",
            "query": {
                "queryState": {
                    "Rows": {
                        "projections": projections
                    }
                }
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def make_matrix(name, x, y, w, h, row_entity, row_col, col_entity, col_col, val_entity, val_measure, title, tab_order=2500):
    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": tab_order, "height": h, "width": w, "tabOrder": tab_order},
        "visual": {
            "visualType": "pivotTable",
            "query": {
                "queryState": {
                    "Rows": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Entity": row_entity}},
                                        "Property": row_col
                                    }
                                },
                                "queryRef": f"{row_entity}.{row_col}"
                            }
                        ]
                    },
                    "Columns": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Entity": col_entity}},
                                        "Property": col_col
                                    }
                                },
                                "queryRef": f"{col_entity}.{col_col}"
                            }
                        ]
                    },
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Measure": {
                                        "Expression": {"SourceRef": {"Entity": val_entity}},
                                        "Property": val_measure
                                    }
                                },
                                "queryRef": f"{val_entity}.{val_measure}"
                            }
                        ]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def make_slicer(name, x, y, w, h, entity, col, title, tab_order=500):
    return {
        "$schema": SCHEMA_VISUAL,
        "name": name,
        "position": {"x": x, "y": y, "z": tab_order, "height": h, "width": w, "tabOrder": tab_order},
        "visual": {
            "visualType": "slicer",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            {
                                "field": {
                                    "Column": {
                                        "Expression": {"SourceRef": {"Entity": entity}},
                                        "Property": col
                                    }
                                },
                                "queryRef": f"{entity}.{col}"
                            }
                        ]
                    }
                }
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": {"expr": {"Literal": {"Value": "true"}}},
                            "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def save_page_and_visuals(page_id, display_name, visuals):
    page_dir = REPORT_DIR / "pages" / page_id
    page_dir.mkdir(parents=True, exist_ok=True)

    page_json = {
        "$schema": SCHEMA_PAGE,
        "name": page_id,
        "displayName": display_name,
        "displayOption": "FitToPage",
        "height": 1080,
        "width": 1920
    }
    (page_dir / "page.json").write_text(json.dumps(page_json, indent=2, ensure_ascii=False), encoding="utf-8")

    visuals_dir = page_dir / "visuals"
    visuals_dir.mkdir(parents=True, exist_ok=True)

    for vis in visuals:
        vis_dir = visuals_dir / vis["name"]
        vis_dir.mkdir(parents=True, exist_ok=True)
        (vis_dir / "visual.json").write_text(json.dumps(vis, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Created page: {display_name} ({page_id}) with {len(visuals)} visuals")


def build_pages():
    pages_meta = []

    # PAGE 1: Executive Market Overview
    p1_id = "fca36af890b0b0475ec0"  # Reuse existing default page id
    p1_visuals = [
        make_textbox("tb_header_01", 40, 20, 1840, 60, "Tiki Kitchen Appliances - Executive Market Overview"),
        make_card("card_rev_01", 40, 100, 340, 120, "fact_product_snapshot", "Total Estimated Revenue", "Doanh thu ước tính (Revenue)", 1000),
        make_card("card_vol_01", 415, 100, 340, 120, "fact_product_snapshot", "Total Volume Sold", "Sản lượng đã bán (Units)", 1100),
        make_card("card_sku_01", 790, 100, 340, 120, "fact_product_snapshot", "Total Active SKUs", "Số SKU hoạt động (SKUs)", 1200),
        make_card("card_rat_01", 1165, 100, 340, 120, "fact_product_snapshot", "Average Rating", "Điểm đánh giá TB (Rating)", 1300),
        make_card("card_cr5_01", 1540, 100, 340, 120, "fact_product_snapshot", "CR5 Index %", "Chỉ số tập trung CR5 (%)", 1400),
        make_bar_chart("bar_subcat_01", 40, 240, 920, 420, "dim_subcategory", "subcategory_name", "fact_product_snapshot", "Total Estimated Revenue", "Doanh thu theo Ngành hàng phụ (Subcategory)", 2000),
        make_treemap("tree_brand_01", 990, 240, 890, 420, "dim_brand", "brand_name", "fact_product_snapshot", "Total Estimated Revenue", "Thị phần Doanh thu theo Thương hiệu (Brand)", 2100),
        make_table("table_alert_01", 40, 680, 1840, 370, [
            ("alerts", "triggered_at", False),
            ("alerts", "product_name", False),
            ("alerts", "brand_name", False),
            ("alerts", "seller_name", False),
            ("alerts", "old_price", False),
            ("alerts", "new_price", False),
            ("alerts", "price_drop_pct", False),
            ("alerts", "savings_amount", False),
        ], "Cảnh báo Giảm giá sốc & Flash Sale gần đây (Real-time Price Drop Alerts)", 3000)
    ]
    save_page_and_visuals(p1_id, "Executive Market Overview", p1_visuals)
    pages_meta.append(p1_id)

    # PAGE 2: Pricing & Discount Saturation
    p2_id = "e102a3b4c5d6e7f8a9b0"
    p2_visuals = [
        make_textbox("tb_header_02", 40, 20, 1840, 60, "Phân tích Điểm bão hòa Giảm giá (Pricing & Discount Saturation - BQ1)"),
        make_slicer("slicer_segment_02", 40, 90, 1840, 80, "fact_product_snapshot", "price_segment", "Bộ lọc Phân khúc Giá (Price Segment Slicer)", 500),
        make_column_chart("col_disc_qty_02", 40, 190, 890, 420, "agg_discount_saturation", "discount_bracket", "agg_discount_saturation", "Avg Units Sold", "Sản lượng bán TB theo Khung giảm giá (Units Sold by Discount Bracket)", 2000),
        make_column_chart("col_disc_sku_02", 960, 190, 920, 420, "agg_discount_saturation", "discount_bracket", "agg_discount_saturation", "Discount SKU Count", "Phân bổ số lượng SKU theo Khung giảm giá (SKU Distribution)", 2100),
        make_scatter_chart("scatter_disc_02", 40, 630, 1840, 420, "fact_product_snapshot", "discount_rate_calc", "fact_product_snapshot", "quantity_sold", "dim_product", "name", "fact_product_snapshot", "Total Estimated Revenue", "Tương quan Tỷ lệ giảm giá vs Sản lượng bán thực tế (Discount % vs Volume)", False, False, 2200)
    ]
    save_page_and_visuals(p2_id, "Pricing & Discount Saturation", p2_visuals)
    pages_meta.append(p2_id)

    # PAGE 3: Brand & Seller Market Share
    p3_id = "b2c3d4e5f6a7b8c9d0e1"
    p3_visuals = [
        make_textbox("tb_header_03", 40, 20, 1840, 60, "Cơ cấu Thị phần Thương hiệu & Nhà bán hàng (Brand & Seller Market Share - BQ2)"),
        make_card("card_cr5_03", 40, 95, 430, 120, "fact_product_snapshot", "CR5 Index %", "Chỉ số tập trung Top 5 Brand (CR5)", 1000),
        make_card("card_rev_03", 510, 95, 430, 120, "fact_product_snapshot", "Total Estimated Revenue", "Tổng Quy mô Doanh thu", 1100),
        make_card("card_vol_03", 980, 95, 430, 120, "fact_product_snapshot", "Total Volume Sold", "Tổng Sản lượng Tiêu thụ", 1200),
        make_card("card_sku_03", 1450, 95, 430, 120, "fact_product_snapshot", "Total Active SKUs", "Tổng số SKU toàn ngành", 1300),
        make_treemap("tree_brand_03", 40, 235, 960, 460, "dim_brand", "brand_name", "fact_product_snapshot", "Total Estimated Revenue", "Thị phần Doanh thu theo Thương hiệu (Brand Revenue Treemap)", 2000),
        make_donut_chart("donut_seller_03", 1040, 235, 840, 460, "dim_seller", "is_official_seller", "fact_product_snapshot", "Total Estimated Revenue", "Tỷ trọng Doanh thu Gian hàng Chính hãng (Official Store vs 3rd Party)", 2100),
        make_table("table_brand_03", 40, 715, 1840, 340, [
            ("dim_brand", "brand_name", False),
            ("fact_product_snapshot", "Total Estimated Revenue", True),
            ("fact_product_snapshot", "Total Volume Sold", True),
            ("fact_product_snapshot", "Total Active SKUs", True),
            ("fact_product_snapshot", "Average Rating", True)
        ], "Bảng Tổng hợp Xếp hạng & Thị phần Thương hiệu (Brand Performance Ranking)", 3000)
    ]
    save_page_and_visuals(p3_id, "Brand & Seller Market Share", p3_visuals)
    pages_meta.append(p3_id)

    # PAGE 4: Customer Perception & Quality Risk Watchlist
    p4_id = "c3d4e5f6a7b8c9d0e1f2"
    p4_visuals = [
        make_textbox("tb_header_04", 40, 20, 1840, 60, "Theo dõi Rủi ro Chất lượng & Trải nghiệm Khách hàng (Quality Risk Watchlist - BQ3)"),
        make_card("card_rev_cnt_04", 40, 95, 430, 120, "fact_reviews", "Total Reviews", "Tổng lượt đánh giá (Reviews)", 1000),
        make_card("card_pos_ratio_04", 510, 95, 430, 120, "fact_reviews", "Positive Review Ratio %", "Tỷ lệ đánh giá Tích cực (4-5★)", 1100),
        make_card("card_neg_ratio_04", 980, 95, 430, 120, "fact_reviews", "Negative Review Ratio %", "Tỷ lệ đánh giá Tiêu cực (1-2★)", 1200),
        make_card("card_avg_rat_04", 1450, 95, 430, 120, "fact_product_snapshot", "Average Rating", "Điểm Rating TB toàn ngành", 1300),
        make_scatter_chart("scatter_qual_04", 40, 235, 960, 460, "fact_product_snapshot", "review_count", "fact_product_snapshot", "rating_average", "dim_product", "name", None, None, "Bản đồ Nhận thức: Số lượt đánh giá vs Điểm Rating (Perception Map)", False, False, 2000),
        make_donut_chart("donut_sent_04", 1040, 235, 840, 460, "fact_reviews", "sentiment_label", "fact_reviews", "Total Reviews", "Cơ cấu Phân loại Cảm xúc Khách hàng (Sentiment Breakdown)", 2100),
        make_table("table_watch_04", 40, 715, 1840, 340, [
            ("dim_product", "name", False),
            ("dim_brand", "brand_name", False),
            ("fact_product_snapshot", "price", False),
            ("fact_product_snapshot", "rating_average", False),
            ("fact_product_snapshot", "review_count", False),
            ("fact_product_snapshot", "quantity_sold", False)
        ], "Danh sách Sản phẩm Cần Giám sát Chất lượng (Quality Risk Watchlist)", 3000)
    ]
    save_page_and_visuals(p4_id, "Customer Perception & Risk Watchlist", p4_visuals)
    pages_meta.append(p4_id)

    # PAGE 5: Assortment & Merchandising Gap Analysis
    p5_id = "d4e5f6a7b8c9d0e1f2a3"
    p5_visuals = [
        make_textbox("tb_header_05", 40, 20, 1840, 60, "Phân tích Khoảng trống Ngành hàng (Assortment & Merchandising Gap Analysis - BQ4)"),
        make_card("card_sku_05", 40, 95, 580, 120, "fact_product_snapshot", "Total Active SKUs", "Tổng số SKU phân tích", 1000),
        make_card("card_rev_05", 670, 95, 580, 120, "fact_product_snapshot", "Total Estimated Revenue", "Tổng Doanh thu Ngành hàng", 1100),
        make_card("card_vol_05", 1300, 95, 580, 120, "fact_product_snapshot", "Total Volume Sold", "Tổng Sản lượng Tiêu thụ", 1200),
        make_matrix("matrix_gap_05", 40, 235, 1100, 460, "dim_subcategory", "subcategory_name", "fact_product_snapshot", "price_segment", "fact_product_snapshot", "Total Active SKUs", "Ma trận Phân khúc Giá x Ngành hàng phụ (Assortment Matrix: SKU Count)", 2000),
        make_column_chart("col_seg_05", 1180, 235, 700, 460, "fact_product_snapshot", "price_segment", "fact_product_snapshot", "Total Active SKUs", "Độ phủ SKU theo Phân khúc Giá (Price Segment Coverage)", 2100),
        make_table("table_gap_05", 40, 715, 1840, 340, [
            ("dim_subcategory", "subcategory_name", False),
            ("fact_product_snapshot", "price_segment", False),
            ("fact_product_snapshot", "Total Active SKUs", True),
            ("fact_product_snapshot", "Total Volume Sold", True),
            ("fact_product_snapshot", "Total Estimated Revenue", True)
        ], "Chi tiết Cơ hội Bổ sung Sản phẩm vào Khoảng trống Danh mục (Gap Action Plan)", 3000)
    ]
    save_page_and_visuals(p5_id, "Assortment Gap Analysis", p5_visuals)
    pages_meta.append(p5_id)

    # PAGE 6: Marketing Priority Matrix
    p6_id = "e5f6a7b8c9d0e1f2a3b4"
    p6_visuals = [
        make_textbox("tb_header_06", 40, 20, 1840, 60, "Ma trận Ưu tiên Tiếp thị & Phân bổ Ngân sách (Marketing Priority Matrix)"),
        make_card("card_rev_06", 40, 95, 580, 120, "fact_product_snapshot", "Total Estimated Revenue", "Tổng Ngân sách / Quy mô Doanh thu", 1000),
        make_card("card_rat_06", 670, 95, 580, 120, "fact_product_snapshot", "Average Rating", "Ngưỡng Rating Chuẩn (Benchmark)", 1100),
        make_card("card_vol_06", 1300, 95, 580, 120, "fact_product_snapshot", "Total Volume Sold", "Tổng Sản lượng Tiêu chuẩn", 1200),
        make_scatter_chart("scatter_quad_06", 40, 235, 1100, 460, "fact_product_snapshot", "rating_average", "fact_product_snapshot", "quantity_sold", "dim_product", "name", "fact_product_snapshot", "Total Estimated Revenue", "Ma trận 4 Góc phần tư (Quadrant Matrix: Rating vs Sales Volume)", False, False, 2000),
        make_table("table_stars_06", 1180, 235, 700, 460, [
            ("dim_product", "name", False),
            ("fact_product_snapshot", "quantity_sold", False),
            ("fact_product_snapshot", "rating_average", False),
            ("fact_product_snapshot", "Total Estimated Revenue", True)
        ], "Top Sản phẩm Ngôi sao cần Đẩy mạnh Chiến dịch (Stars & Opportunities)", 2100),
        make_table("table_all_strat_06", 40, 715, 1840, 340, [
            ("dim_product", "name", False),
            ("dim_brand", "brand_name", False),
            ("dim_subcategory", "subcategory_name", False),
            ("fact_product_snapshot", "price", False),
            ("fact_product_snapshot", "quantity_sold", False),
            ("fact_product_snapshot", "rating_average", False),
            ("fact_product_snapshot", "Total Estimated Revenue", True)
        ], "Chiến lược Định vị Sản phẩm Toàn diện (Promote / Nurture / Delist / Audit)", 3000)
    ]
    save_page_and_visuals(p6_id, "Marketing Priority Matrix", p6_visuals)
    pages_meta.append(p6_id)

    # PAGE 7: Voice of Customer (VoC) Root-Causes
    p7_id = "f6a7b8c9d0e1f2a3b4c5"
    p7_visuals = [
        make_textbox("tb_header_07", 40, 20, 1840, 60, "Phân tích Tiếng nói Khách hàng & Nguyên nhân Khiếu nại (Voice of Customer VoC - BQ6)"),
        make_card("card_aftersales_07", 40, 95, 430, 120, "fact_reviews", "AfterSales Complaint %", "Khiếu nại Hậu cần & Đổi trả (%)", 1000),
        make_card("card_defect_07", 510, 95, 430, 120, "fact_reviews", "Hardware Defect %", "Lỗi Phần cứng & Kỹ thuật (%)", 1100),
        make_card("card_tot_rev_07", 980, 95, 430, 120, "fact_reviews", "Total Reviews", "Tổng số Review NLP Phân tích", 1200),
        make_card("card_neg_share_07", 1450, 95, 430, 120, "fact_reviews", "Negative Review Ratio %", "Tỷ lệ Đánh giá Tiêu cực (1-2★)", 1300),
        make_donut_chart("donut_voc_07", 40, 235, 890, 460, "agg_voc_complaint_causes", "complaint_category", "agg_voc_complaint_causes", "Total Complaint Count", "Cơ cấu Khiếu nại Tiêu cực: CSKH/Hậu cần vs Lỗi phần cứng (Donut Chart)", 2000),
        make_bar_chart("bar_voc_07", 970, 235, 910, 460, "agg_voc_complaint_causes", "complaint_category", "agg_voc_complaint_causes", "Complaint % Share", "Tỷ trọng Khiếu nại trong nhóm Đánh giá Tiêu cực (% Share of 1-2★)", 2100),
        make_table("table_nlp_07", 40, 715, 1840, 340, [
            ("fact_reviews", "review_id", False),
            ("fact_reviews", "rating", False),
            ("fact_reviews", "sentiment_label", False),
            ("fact_reviews", "tokenized_text", False),
            ("fact_reviews", "is_after_sales_complaint", False),
            ("fact_reviews", "is_defect_complaint", False),
            ("fact_reviews", "thank_count", False)
        ], "Chi tiết Phân tích Cảm xúc & Tách từ Tiếng Việt (Vietnamese NLP Tokenized Feedback)", 3000)
    ]
    save_page_and_visuals(p7_id, "Voice of Customer Root-Causes", p7_visuals)
    pages_meta.append(p7_id)

    # Update pages.json
    pages_json = {
        "$schema": SCHEMA_PAGES,
        "pageOrder": pages_meta,
        "activePageName": pages_meta[0]
    }
    (REPORT_DIR / "pages" / "pages.json").write_text(json.dumps(pages_json, indent=2), encoding="utf-8")
    print(f"Updated pages.json with {len(pages_meta)} pages in order")


if __name__ == "__main__":
    print("=== Configuring Semantic Model TMDL & Measures ===")
    update_fact_product_snapshot()
    update_fact_reviews()
    update_agg_tables()
    update_relationships()

    print("\n=== Building PBIP Report Pages & Visuals ===")
    build_pages()
    print("\nPBIP Dashboard Generation Completed Successfully!")
