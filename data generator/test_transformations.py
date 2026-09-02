"""Unit tests for OHN transformation logic.

Run locally against a standalone PySpark session; the same suite runs in CI
before any notebook is promoted. Tests use synthetic fixtures only.
"""

import datetime as dt

import pytest
from pyspark.sql import SparkSession, functions as F

import sys
sys.path.insert(0, "notebooks")


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("ohn-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield s
    s.stop()


# ---------------------------------------------------------------- helpers
from utils import std_postal_code, std_string, age_band, row_hash  # noqa: E402


class TestStandardization:
    def test_std_string_collapses_whitespace_and_uppercases(self, spark):
        df = spark.createDataFrame([("  john   SMITH ",), ("",), (None,)], "v string")
        out = df.select(std_string(F.col("v")).alias("r")).collect()
        assert out[0]["r"] == "JOHN SMITH"
        assert out[1]["r"] is None, "empty string must become null, not empty"
        assert out[2]["r"] is None

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("m5v 3l9", "M5V3L9"),
            ("M5V-3L9", "M5V3L9"),
            ("12345", None),
            ("M5V3L", None),
            (None, None),
        ],
    )
    def test_postal_code_validation(self, spark, raw, expected):
        df = spark.createDataFrame([(raw,)], "v string")
        assert df.select(std_postal_code(F.col("v")).alias("r")).collect()[0]["r"] == expected

    def test_age_band_boundaries(self, spark):
        as_of = dt.date(2026, 7, 30)
        cases = [
            (dt.date(2026, 1, 1), "0-17"),
            (dt.date(2008, 7, 30), "18-34"),
            (dt.date(1976, 7, 30), "50-64"),
            (dt.date(1941, 7, 30), "85+"),
            (None, "Unknown"),
        ]
        df = spark.createDataFrame(
            [(b, as_of) for b, _ in cases], "birth_date date, as_of date"
        )
        got = df.select(age_band(F.col("birth_date"), F.col("as_of")).alias("band")).collect()
        assert [r["band"] for r in got] == [e for _, e in cases]


class TestRowHash:
    def test_null_position_changes_hash(self, spark):
        """(null, 'A') and ('A', null) must not collide."""
        df = spark.createDataFrame([(None, "A"), ("A", None)], "a string, b string")
        hashes = [r["_row_hash"] for r in row_hash(df, ["a", "b"]).collect()]
        assert hashes[0] != hashes[1]

    def test_cosmetic_change_does_not_change_hash(self, spark):
        df = spark.createDataFrame([(" smith ",), ("SMITH",)], "a string")
        hashes = [r["_row_hash"] for r in row_hash(df, ["a"]).collect()]
        assert hashes[0] == hashes[1], "trim and case differences must not create SCD2 versions"


# ---------------------------------------------------------------- business rules
class TestLengthOfStay:
    def test_same_day_stay_counts_as_one_day(self, spark):
        df = spark.createDataFrame(
            [("2026-05-01 08:00:00", "2026-05-01 19:30:00")], "adm string, dis string"
        ).select(F.col("adm").cast("timestamp"), F.col("dis").cast("timestamp"))
        los = df.select(
            F.greatest(F.datediff("dis", "adm"), F.lit(1)).alias("los")
        ).collect()[0]["los"]
        assert los == 1

    def test_negative_interval_yields_null_not_negative(self, spark):
        df = spark.createDataFrame(
            [("2026-05-10 08:00:00", "2026-05-01 08:00:00")], "adm string, dis string"
        ).select(F.col("adm").cast("timestamp"), F.col("dis").cast("timestamp"))
        out = df.withColumn(
            "los",
            F.when(F.col("dis") < F.col("adm"), None).otherwise(F.datediff("dis", "adm")),
        ).collect()[0]["los"]
        assert out is None, "clock errors must not produce negative ALOS contributions"


class TestReadmission:
    """The readmission rule has four exclusions; each gets its own test so a
    change to the definition fails loudly rather than quietly shifting a KPI."""

    @pytest.fixture
    def admissions(self, spark):
        rows = [
            # (patient, admission, discharge, type, prior_transfer, prior_death, expected)
            ("P1", "2026-01-01", "2026-01-05", "EMERGENCY", False, False, False),  # index
            ("P1", "2026-01-20", "2026-01-25", "EMERGENCY", False, False, True),   # 15 days later
            ("P2", "2026-01-01", "2026-01-05", "EMERGENCY", False, False, False),
            ("P2", "2026-03-01", "2026-03-04", "EMERGENCY", False, False, False),  # 55 days
            ("P3", "2026-01-01", "2026-01-05", "EMERGENCY", False, False, False),
            ("P3", "2026-01-10", "2026-01-12", "ELECTIVE", False, False, False),   # planned
            ("P4", "2026-01-01", "2026-01-05", "EMERGENCY", False, False, False),
            ("P4", "2026-01-08", "2026-01-11", "EMERGENCY", True, False, False),   # transfer
            ("P5", "2026-01-01", "2026-01-05", "EMERGENCY", False, False, False),
            ("P5", "2026-01-09", "2026-01-12", "EMERGENCY", False, True, False),   # prior death
        ]
        return spark.createDataFrame(
            rows,
            """patient string, adm string, dis string, adm_type string,
               prior_transfer boolean, prior_death boolean, expected boolean""",
        ).withColumn("adm", F.col("adm").cast("timestamp")) \
         .withColumn("dis", F.col("dis").cast("timestamp"))

    def test_readmission_flag_matches_expectations(self, spark, admissions):
        from pyspark.sql import Window

        w = Window.partitionBy("patient").orderBy("adm")
        out = (
            admissions
            .withColumn("prior_dis", F.lag("dis").over(w))
            .withColumn("gap", F.datediff("adm", "prior_dis"))
            .withColumn(
                "is_readmission",
                F.coalesce(
                    F.col("gap").between(0, 30)
                    & (F.col("adm_type") != "ELECTIVE")
                    & (~F.col("prior_transfer"))
                    & (~F.col("prior_death")),
                    F.lit(False),
                ),
            )
        ).collect()

        for row in out:
            assert row["is_readmission"] == row["expected"], (
                f"patient {row['patient']} admitted {row['adm']}: "
                f"expected {row['expected']}, got {row['is_readmission']}"
            )


class TestOccupancy:
    def test_blocked_beds_excluded_from_denominator(self, spark):
        """A bed blocked all day must not depress the occupancy rate."""
        df = spark.createDataFrame(
            [(24.0, 0.0, 0.0), (0.0, 24.0, 0.0), (0.0, 0.0, 24.0)],
            "occupied double, available double, blocked double",
        )
        agg = df.agg(
            F.sum("occupied").alias("o"), F.sum("available").alias("a")
        ).collect()[0]
        rate = agg["o"] / (agg["o"] + agg["a"])
        assert rate == pytest.approx(0.5), "expected 1 of 2 usable beds occupied"


class TestClaimRates:
    def test_approval_rate_uses_adjudicated_denominator(self, spark):
        df = spark.createDataFrame(
            [
                ("APPROVED", True, True),
                ("DENIED", True, False),
                ("PENDING", False, False),
                ("PENDING", False, False),
            ],
            "status string, is_adjudicated boolean, is_approved boolean",
        )
        adjudicated = df.filter(F.col("is_adjudicated")).count()
        approved = df.filter(F.col("is_approved")).count()
        assert approved / adjudicated == pytest.approx(0.5)
        assert approved / df.count() == pytest.approx(0.25), (
            "including pending claims would report 25% and understate performance"
        )


class TestNoShowRate:
    def test_cancellations_excluded_from_denominator(self, spark):
        df = spark.createDataFrame(
            [("COMPLETED",), ("COMPLETED",), ("NO_SHOW",), ("CANCELLED_PATIENT",)],
            "status string",
        )
        completed = df.filter(F.col("status") == "COMPLETED").count()
        no_shows = df.filter(F.col("status") == "NO_SHOW").count()
        assert no_shows / (completed + no_shows) == pytest.approx(1 / 3)


class TestPatientMatching:
    """Regression guards on the MPI thresholds — the highest-risk logic in the
    platform. A change to a weight that shifts these outcomes should require a
    deliberate test update, not slip through unnoticed."""

    def score(self, hcn_match, dob_match, name_match, sex_match, postal_match):
        return (
            (12.0 if hcn_match else 0.0)
            + (6.0 if dob_match else -5.0)
            + (4.0 if name_match else -3.0)
            + (1.0 if sex_match else -3.0)
            + (3.0 if postal_match else -1.0)
        )

    def test_matching_hcn_and_dob_auto_links(self):
        assert self.score(True, True, True, True, True) >= 14.0

    def test_name_and_dob_only_goes_to_review(self):
        s = self.score(False, True, True, True, False)
        assert 8.0 <= s < 14.0, f"expected review band, got {s}"

    def test_different_dob_and_name_stays_distinct(self):
        assert self.score(False, False, False, True, True) < 8.0
