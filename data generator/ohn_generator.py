#!/usr/bin/env python3
"""
Ontario Healthcare Network — synthetic source data generator.

Produces CSV files shaped like the real source systems described in
docs/02-data-model-and-mapping.md, laid out in the OneLake landing-zone
folder structure.

Design principle: the data is deliberately imperfect. A clean synthetic
dataset makes the MPI match nothing, every DQ rule pass at 100%, and every
report show a flat line — the platform would look like it works while
proving nothing. Defect rates are configurable at the top of this file and
every injected defect is recorded in the _truth files so you can measure how
well the platform catches them.

Standard library only — runs locally or inside a Fabric notebook unchanged.

Usage:
    python ohn_generator.py --patients 5000 --out ./landing
    python ohn_generator.py --patients 20000 --out ./landing --start 2023-01-01
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

# =====================================================================
# CONFIGURATION
# =====================================================================

SEED = 20260801

# --- Defect injection rates -----------------------------------------
# These are what make the platform's MDM and DQ logic worth building.
DEFECTS = {
    "intra_source_duplicate": 0.04,   # same person registered twice in the EHR
    "name_typo": 0.12,                # transposition/substitution in a name
    "nickname_used": 0.15,            # Robert -> Bob in a secondary system
    "missing_hcn": 0.08,              # no health card captured
    "invalid_hcn": 0.02,              # fails the mod-10 check digit
    "missing_dob": 0.015,             # DQ-PAT-001
    "impossible_dob": 0.003,          # DQ-PAT-002 — future or pre-1900
    "malformed_postal": 0.05,         # DQ-PAT-005
    "missing_postal": 0.06,
    "stale_address": 0.20,            # older source holds a previous address
    "unmapped_sex_code": 0.01,        # source code not in ref_code_mapping
    "clock_error_discharge": 0.004,   # DQ-ADM-003 — discharge before admission
    "clock_error_ed": 0.006,          # DQ-ED-002 — milestones out of sequence
    "impossible_ctas": 0.004,         # DQ-ED-001 — CTAS outside 1..5
    "negative_lab_turnaround": 0.003, # DQ-LAB-003
    "unmapped_loinc": 0.015,          # DQ-LAB-001
    "unmapped_din": 0.012,            # DQ-MED-001
    "claim_over_approval": 0.004,     # DQ-CLM-002 — approved > billed
    "billing_line_imbalance": 0.002,  # DQ-BIL-001
    "survey_score_out_of_range": 0.005,
    "duplicate_file_delivery": 0.0,   # set >0 to test the Bronze dedup guard
}

# --- Clinical / operational realism ---------------------------------
CTAS_DISTRIBUTION = {1: 0.012, 2: 0.148, 3: 0.452, 4: 0.298, 5: 0.090}

# Median PIA wait in minutes by CTAS level. Higher acuity is seen sooner.
CTAS_PIA_MEDIAN = {1: 5, 2: 22, 3: 78, 4: 132, 5: 165}

# Log-normal LOS parameters (mu, sigma of the underlying normal) by service line
LOS_PARAMS = {
    "Cardiology": (1.55, 0.62),
    "General Surgery": (1.32, 0.58),
    "Internal Medicine": (1.70, 0.70),
    "Orthopaedics": (1.28, 0.55),
    "Obstetrics": (0.92, 0.42),
    "Paediatrics": (1.05, 0.60),
    "Oncology": (1.78, 0.75),
    "Neurology": (1.62, 0.68),
    "Respirology": (1.66, 0.66),
    "Rehabilitation": (2.45, 0.55),
}

BASE_READMISSION_RATE = 0.112
BASE_NO_SHOW_RATE = 0.094
BASE_CANCELLATION_RATE = 0.118
BASE_DENIAL_RATE = 0.096
BASE_LWBS_RATE = 0.031

SOURCE_SYSTEMS = ["EHR", "SCHED", "FIN"]

# =====================================================================
# NAME AND PLACE POOLS
# =====================================================================

GIVEN_M = ["James", "Robert", "Michael", "David", "William", "Richard", "Joseph",
           "Thomas", "Christopher", "Daniel", "Matthew", "Anthony", "Mark", "Steven",
           "Andrew", "Kenneth", "Joshua", "Kevin", "Brian", "George", "Wei", "Ahmed",
           "Mohammed", "Raj", "Amir", "Jean", "Luc", "Diego", "Ivan", "Nikolai",
           "Hassan", "Omar", "Yusuf", "Chen", "Jin", "Hiroshi", "Tomas", "Pedro"]

GIVEN_F = ["Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan",
           "Jessica", "Sarah", "Karen", "Nancy", "Lisa", "Margaret", "Betty",
           "Sandra", "Ashley", "Emily", "Michelle", "Amanda", "Melissa", "Mei",
           "Fatima", "Aisha", "Priya", "Sofia", "Marie", "Claire", "Ana", "Olga",
           "Yuki", "Ling", "Zara", "Nadia", "Ingrid", "Chiara", "Amara"]

FAMILY = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
          "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
          "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
          "Lee", "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson",
          "Walker", "Young", "Allen", "King", "Wright", "Scott", "Hill", "Green",
          "Adams", "Baker", "Nelson", "Carter", "Mitchell", "Patel", "Singh",
          "Kumar", "Chen", "Wang", "Li", "Zhang", "Nguyen", "Tran", "Kim", "Park",
          "Ali", "Khan", "Ahmed", "Hassan", "Osei", "Mensah", "Okafor", "Tremblay",
          "Gagnon", "Roy", "Cote", "Bouchard", "Kowalski", "Nowak", "Silva",
          "Santos", "Ferreira", "Rossi", "Ricci", "Murphy", "Kelly", "O'Brien",
          "MacDonald", "Campbell", "Stewart", "Fraser", "Ross", "Sutherland"]

NICKNAMES = {
    "Robert": "Bob", "William": "Bill", "Richard": "Rick", "James": "Jim",
    "Michael": "Mike", "Christopher": "Chris", "Joseph": "Joe", "Thomas": "Tom",
    "Daniel": "Dan", "Matthew": "Matt", "Anthony": "Tony", "Steven": "Steve",
    "Andrew": "Andy", "Kenneth": "Ken", "Joshua": "Josh", "Elizabeth": "Liz",
    "Jennifer": "Jen", "Patricia": "Pat", "Margaret": "Maggie", "Barbara": "Barb",
    "Susan": "Sue", "Jessica": "Jess", "Sandra": "Sandy", "Amanda": "Mandy",
    "Michelle": "Shelly", "Nancy": "Nan", "Katherine": "Kate", "Alexander": "Alex",
}

LANGUAGES = (["English"] * 68 + ["French"] * 8 + ["Mandarin"] * 5 + ["Cantonese"] * 3
             + ["Punjabi"] * 3 + ["Urdu"] * 2 + ["Tagalog"] * 2 + ["Spanish"] * 2
             + ["Arabic"] * 2 + ["Portuguese"] + ["Italian"] + ["Tamil"] + ["Farsi"])

# Toronto-area forward sortation areas, weighted loosely by population
FSA_POOL = ["M1B", "M1C", "M1E", "M1G", "M1H", "M1J", "M1K", "M1L", "M1M", "M1N",
            "M2H", "M2J", "M2K", "M2M", "M2N", "M2R", "M3A", "M3B", "M3C", "M3H",
            "M4A", "M4B", "M4C", "M4E", "M4G", "M4H", "M4J", "M4K", "M4L", "M4M",
            "M5A", "M5B", "M5E", "M5G", "M5H", "M5J", "M5K", "M5N", "M5P", "M5R",
            "M6A", "M6B", "M6C", "M6E", "M6G", "M6H", "M6J", "M6K", "M6L", "M6M",
            "M8V", "M8W", "M8X", "M8Y", "M9A", "M9B", "M9C", "M9L", "M9M", "M9N",
            "L4B", "L4C", "L4J", "L4K", "L4L", "L5A", "L5B", "L5C", "L5N", "L6A",
            "L6B", "L6C", "L6G", "L6H", "L6J", "L6L", "L6M", "L6P", "L6R", "L6S"]

STREET_NAMES = ["Maple", "Oak", "Cedar", "Elm", "Birch", "Willow", "Pine", "Spruce",
                "King", "Queen", "Front", "Bay", "Yonge", "Bloor", "College",
                "Dundas", "Adelaide", "Richmond", "Wellington", "Church", "Jarvis",
                "Sherbourne", "Parliament", "Bathurst", "Spadina", "Dufferin"]
STREET_TYPES = ["St", "Ave", "Rd", "Dr", "Cres", "Blvd", "Way", "Pl", "Ct", "Ln"]


# =====================================================================
# HELPERS
# =====================================================================

def luhn_check_digit(digits: str) -> int:
    """Mod-10 check digit, used for Ontario health card numbers."""
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return (10 - total % 10) % 10


def make_hcn(rng: random.Random, valid: bool = True) -> str:
    """10-digit Ontario-style health card number with a mod-10 check digit."""
    body = "".join(str(rng.randint(0, 9)) for _ in range(9))
    check = luhn_check_digit(body + "0")
    # luhn_check_digit expects the full string with a placeholder; recompute
    # against the 9-digit body positioned correctly
    for candidate in range(10):
        trial = body + str(candidate)
        total = 0
        for i, ch in enumerate(reversed(trial)):
            d = int(ch)
            if i % 2 == 1:
                d *= 2
                if d > 9:
                    d -= 9
            total += d
        if total % 10 == 0:
            check = candidate
            break
    hcn = body + str(check)
    if not valid:
        # Flip the check digit to something wrong
        bad = (check + rng.randint(1, 9)) % 10
        hcn = body + str(bad)
    return hcn


def typo(rng: random.Random, s: str) -> str:
    """Apply one realistic data-entry error: transposition, drop, or substitution."""
    if len(s) < 3:
        return s
    mode = rng.choice(["transpose", "drop", "substitute", "double"])
    i = rng.randrange(1, len(s) - 1)
    if mode == "transpose":
        return s[:i] + s[i + 1] + s[i] + s[i + 2:]
    if mode == "drop":
        return s[:i] + s[i + 1:]
    if mode == "double":
        return s[:i] + s[i] + s[i:]
    neighbours = {"a": "s", "e": "r", "i": "o", "o": "i", "n": "m", "m": "n",
                  "s": "a", "t": "r", "r": "t", "l": "k", "c": "v", "b": "v"}
    ch = s[i].lower()
    return s[:i] + neighbours.get(ch, ch) + s[i + 1:]


def make_postal(rng: random.Random, fsa: str) -> str:
    letters = "ABCEGHJKLMNPRSTVWXYZ"
    return f"{fsa}{rng.randint(0,9)}{rng.choice(letters)}{rng.randint(0,9)}"


def make_phone(rng: random.Random) -> str:
    area = rng.choice(["416", "647", "437", "905", "289", "365"])
    return f"{area}{rng.randint(2,9)}{rng.randint(0,9)}{rng.randint(0,9)}" \
           f"{rng.randint(0,9):d}{rng.randint(0,9)}{rng.randint(0,9)}{rng.randint(0,9)}"


def lognormal_days(rng: random.Random, mu: float, sigma: float,
                   floor: int = 1, ceil: int = 240) -> int:
    v = math.exp(rng.gauss(mu, sigma))
    return max(floor, min(ceil, int(round(v))))


def weighted_choice(rng: random.Random, mapping: dict):
    keys = list(mapping.keys())
    weights = list(mapping.values())
    return rng.choices(keys, weights=weights, k=1)[0]


def diurnal_hour(rng: random.Random) -> int:
    """ED arrival hour. Bimodal: late-morning peak and an evening peak."""
    weights = [3, 2, 2, 1, 1, 2, 3, 5, 7, 9, 10, 10, 9, 8, 8, 8, 9, 10, 10, 9, 7, 6, 5, 4]
    return rng.choices(range(24), weights=weights, k=1)[0]


def business_hour(rng: random.Random) -> tuple[int, int]:
    hour = rng.choices(range(8, 18), weights=[6, 9, 10, 9, 5, 8, 10, 9, 7, 4], k=1)[0]
    minute = rng.choice([0, 15, 30, 45])
    return hour, minute


def fmt_ts(d: datetime) -> str:
    return d.strftime("%Y-%m-%d %H:%M:%S")


def fmt_d(d: date) -> str:
    return d.strftime("%Y-%m-%d")


# =====================================================================
# DOMAIN OBJECTS
# =====================================================================

@dataclass
class Person:
    """The ground truth: one real human being.

    Source systems each hold a partial, possibly garbled view of this person.
    The MPI's job is to reconstruct it. person_id is written to the _truth
    file so MDM precision and recall can be measured.
    """
    person_id: str
    given_name: str
    family_name: str
    birth_date: date
    sex: str
    hcn: str
    fsa: str
    postal_code: str
    prior_postal_code: str
    phone: str
    prior_phone: str
    language: str
    is_deceased: bool = False
    deceased_date: date | None = None
    source_ids: dict = field(default_factory=dict)


@dataclass
class SourceRecord:
    """One person's representation inside one source system."""
    source_system: str
    source_patient_id: str
    person_id: str
    given_name: str
    family_name: str
    birth_date: str
    sex_code: str
    hcn: str
    postal_code: str
    phone: str
    language: str
    updated_ts: str
    is_injected_duplicate: bool = False


# =====================================================================
# REFERENCE DATA
# =====================================================================

HOSPITALS = [
    ("H001", "Lakeshore General Hospital", "Acute", "Toronto Central", "Toronto", 420, 1, 1),
    ("H002", "Northview Medical Centre", "Acute", "Toronto North", "North York", 310, 1, 0),
    ("H003", "Riverside Community Hospital", "Community", "Toronto West", "Etobicoke", 185, 1, 0),
    ("H004", "Highland Rehabilitation Institute", "Rehab", "Toronto East", "Scarborough", 120, 0, 0),
    ("H005", "Bayview Ambulatory Clinic", "Clinic", "Toronto Central", "Toronto", 0, 0, 0),
]

DEPARTMENTS = [
    ("D01", "Cardiology", "Cardiology", 1, 1),
    ("D02", "General Surgery", "General Surgery", 1, 1),
    ("D03", "Internal Medicine", "Internal Medicine", 1, 1),
    ("D04", "Orthopaedics", "Orthopaedics", 1, 1),
    ("D05", "Obstetrics", "Obstetrics", 1, 1),
    ("D06", "Paediatrics", "Paediatrics", 1, 1),
    ("D07", "Oncology", "Oncology", 1, 1),
    ("D08", "Neurology", "Neurology", 1, 1),
    ("D09", "Respirology", "Respirology", 1, 1),
    ("D10", "Rehabilitation", "Rehabilitation", 1, 1),
    ("D11", "Emergency", "Emergency", 1, 0),
    ("D12", "Laboratory", "Diagnostics", 0, 0),
    ("D13", "Pharmacy", "Support", 0, 0),
    ("D14", "Radiology", "Diagnostics", 0, 0),
]

WARD_TYPES = ["Med-Surg", "ICU", "Maternity", "Rehab", "ED", "Step-Down"]

SPECIALTIES = ["Cardiology", "General Surgery", "Internal Medicine", "Orthopaedics",
               "Obstetrics", "Paediatrics", "Oncology", "Neurology", "Respirology",
               "Emergency Medicine", "Anaesthesiology", "Physiatry"]

# ICD-10-CA style codes. Descriptions are abbreviated for readability.
DIAGNOSES = [
    ("I50.0", "Congestive heart failure", "Circulatory system", "Heart failure", 1),
    ("I21.9", "Acute myocardial infarction, unspecified", "Circulatory system", "Ischaemic heart disease", 0),
    ("I10", "Essential hypertension", "Circulatory system", "Hypertensive diseases", 1),
    ("I48.0", "Paroxysmal atrial fibrillation", "Circulatory system", "Arrhythmia", 1),
    ("J44.1", "COPD with acute exacerbation", "Respiratory system", "Chronic lower respiratory", 1),
    ("J18.9", "Pneumonia, unspecified organism", "Respiratory system", "Influenza and pneumonia", 0),
    ("J45.9", "Asthma, unspecified", "Respiratory system", "Chronic lower respiratory", 1),
    ("E11.9", "Type 2 diabetes mellitus without complications", "Endocrine", "Diabetes mellitus", 1),
    ("E11.2", "Type 2 diabetes with kidney complications", "Endocrine", "Diabetes mellitus", 1),
    ("N18.3", "Chronic kidney disease, stage 3", "Genitourinary", "Renal failure", 1),
    ("N39.0", "Urinary tract infection, site unspecified", "Genitourinary", "Other urinary", 0),
    ("K80.2", "Calculus of gallbladder without cholecystitis", "Digestive system", "Gallbladder", 0),
    ("K35.8", "Acute appendicitis, unspecified", "Digestive system", "Appendix", 0),
    ("K92.2", "Gastrointestinal haemorrhage, unspecified", "Digestive system", "Other digestive", 0),
    ("M17.1", "Unilateral primary osteoarthritis of knee", "Musculoskeletal", "Arthropathies", 1),
    ("M16.1", "Unilateral primary osteoarthritis of hip", "Musculoskeletal", "Arthropathies", 1),
    ("S72.0", "Fracture of neck of femur", "Injury", "Fracture of femur", 0),
    ("S06.0", "Concussion", "Injury", "Intracranial injury", 0),
    ("I63.9", "Cerebral infarction, unspecified", "Circulatory system", "Cerebrovascular", 0),
    ("G40.9", "Epilepsy, unspecified", "Nervous system", "Episodic disorders", 1),
    ("F32.1", "Major depressive disorder, moderate", "Mental health", "Mood disorders", 1),
    ("F20.9", "Schizophrenia, unspecified", "Mental health", "Psychotic disorders", 1),
    ("C50.9", "Malignant neoplasm of breast, unspecified", "Neoplasms", "Breast cancer", 1),
    ("C34.9", "Malignant neoplasm of bronchus or lung", "Neoplasms", "Lung cancer", 1),
    ("C18.9", "Malignant neoplasm of colon, unspecified", "Neoplasms", "Colorectal cancer", 1),
    ("O80", "Single spontaneous delivery", "Pregnancy and childbirth", "Delivery", 0),
    ("O82", "Single delivery by caesarean section", "Pregnancy and childbirth", "Delivery", 0),
    ("A41.9", "Sepsis, unspecified organism", "Infectious disease", "Sepsis", 0),
    ("R07.4", "Chest pain, unspecified", "Symptoms and signs", "Chest symptoms", 0),
    ("R10.4", "Abdominal pain, other and unspecified", "Symptoms and signs", "Abdominal symptoms", 0),
    ("Z51.1", "Encounter for antineoplastic chemotherapy", "Factors influencing health", "Aftercare", 0),
    ("U07.1", "COVID-19, virus identified", "Infectious disease", "Viral infection", 0),
]

LAB_TESTS = [
    ("2345-7", "Glucose, serum", "Chemistry", "Serum", "mmol/L", 3.9, 5.6, 1),
    ("2160-0", "Creatinine, serum", "Chemistry", "Serum", "umol/L", 50.0, 110.0, 1),
    ("3094-0", "Urea nitrogen, serum", "Chemistry", "Serum", "mmol/L", 2.5, 8.0, 1),
    ("2951-2", "Sodium, serum", "Chemistry", "Serum", "mmol/L", 135.0, 145.0, 1),
    ("2823-3", "Potassium, serum", "Chemistry", "Serum", "mmol/L", 3.5, 5.1, 1),
    ("2075-0", "Chloride, serum", "Chemistry", "Serum", "mmol/L", 98.0, 107.0, 1),
    ("718-7", "Haemoglobin", "Haematology", "Whole blood", "g/L", 120.0, 160.0, 1),
    ("4544-3", "Haematocrit", "Haematology", "Whole blood", "%", 36.0, 46.0, 1),
    ("6690-2", "Leukocytes", "Haematology", "Whole blood", "10*9/L", 4.0, 11.0, 1),
    ("777-3", "Platelets", "Haematology", "Whole blood", "10*9/L", 150.0, 400.0, 1),
    ("1975-2", "Bilirubin, total", "Chemistry", "Serum", "umol/L", 3.0, 20.0, 0),
    ("1742-6", "Alanine aminotransferase", "Chemistry", "Serum", "U/L", 7.0, 56.0, 0),
    ("1920-8", "Aspartate aminotransferase", "Chemistry", "Serum", "U/L", 10.0, 40.0, 0),
    ("2093-3", "Cholesterol, total", "Chemistry", "Serum", "mmol/L", 0.0, 5.2, 0),
    ("2085-9", "HDL cholesterol", "Chemistry", "Serum", "mmol/L", 1.0, 2.5, 0),
    ("4548-4", "Haemoglobin A1c", "Chemistry", "Whole blood", "%", 4.0, 6.0, 0),
    ("3016-3", "Thyrotropin (TSH)", "Chemistry", "Serum", "mIU/L", 0.4, 4.0, 0),
    ("10839-9", "Troponin I, cardiac", "Chemistry", "Serum", "ng/L", 0.0, 14.0, 1),
    ("30934-4", "Natriuretic peptide B", "Chemistry", "Plasma", "ng/L", 0.0, 100.0, 1),
    ("5902-2", "Prothrombin time", "Coagulation", "Plasma", "s", 11.0, 13.5, 1),
    ("6301-6", "INR", "Coagulation", "Plasma", "ratio", 0.8, 1.2, 1),
    ("1988-5", "C-reactive protein", "Chemistry", "Serum", "mg/L", 0.0, 5.0, 1),
    ("600-7", "Blood culture", "Microbiology", "Whole blood", None, None, None, 1),
    ("630-4", "Urine culture", "Microbiology", "Urine", None, None, None, 0),
    ("5811-5", "Urine specific gravity", "Urinalysis", "Urine", "ratio", 1.005, 1.030, 0),
]

MEDICATIONS = [
    ("02245678", "Metformin", "Glucophage", "A10BA02", "Antidiabetics", "Tablet", "PO", 0, 0, 1),
    ("02123456", "Ramipril", "Altace", "C09AA05", "ACE inhibitors", "Capsule", "PO", 0, 0, 1),
    ("02234567", "Atorvastatin", "Lipitor", "C10AA05", "Statins", "Tablet", "PO", 0, 0, 1),
    ("02345671", "Metoprolol", "Lopressor", "C07AB02", "Beta blockers", "Tablet", "PO", 0, 0, 1),
    ("02456712", "Furosemide", "Lasix", "C03CA01", "Loop diuretics", "Tablet", "PO", 0, 0, 1),
    ("02567123", "Warfarin", "Coumadin", "B01AA03", "Anticoagulants", "Tablet", "PO", 0, 1, 1),
    ("02671234", "Apixaban", "Eliquis", "B01AF02", "Anticoagulants", "Tablet", "PO", 0, 1, 1),
    ("02712345", "Heparin", "Hepalean", "B01AB01", "Anticoagulants", "Injection", "IV", 0, 1, 1),
    ("02812345", "Insulin glargine", "Lantus", "A10AE04", "Insulins", "Injection", "SC", 0, 1, 1),
    ("02912345", "Salbutamol", "Ventolin", "R03AC02", "Beta-2 agonists", "Inhaler", "INH", 0, 0, 1),
    ("03012345", "Prednisone", "Deltasone", "H02AB07", "Corticosteroids", "Tablet", "PO", 0, 0, 1),
    ("03112345", "Amoxicillin", "Amoxil", "J01CA04", "Penicillins", "Capsule", "PO", 0, 0, 1),
    ("03212345", "Ceftriaxone", "Rocephin", "J01DD04", "Cephalosporins", "Injection", "IV", 0, 0, 1),
    ("03312345", "Vancomycin", "Vancocin", "J01XA01", "Glycopeptides", "Injection", "IV", 0, 1, 1),
    ("03412345", "Piperacillin-tazobactam", "Tazocin", "J01CR05", "Penicillins", "Injection", "IV", 0, 0, 1),
    ("03512345", "Morphine", "Statex", "N02AA01", "Opioid analgesics", "Injection", "IV", 1, 1, 1),
    ("03612345", "Hydromorphone", "Dilaudid", "N02AA03", "Opioid analgesics", "Tablet", "PO", 1, 1, 1),
    ("03712345", "Acetaminophen", "Tylenol", "N02BE01", "Non-opioid analgesics", "Tablet", "PO", 0, 0, 1),
    ("03812345", "Ibuprofen", "Advil", "M01AE01", "NSAIDs", "Tablet", "PO", 0, 0, 1),
    ("03912345", "Pantoprazole", "Pantoloc", "A02BC02", "Proton pump inhibitors", "Tablet", "PO", 0, 0, 1),
    ("04012345", "Ondansetron", "Zofran", "A04AA01", "Antiemetics", "Tablet", "PO", 0, 0, 1),
    ("04112345", "Sertraline", "Zoloft", "N06AB06", "SSRIs", "Tablet", "PO", 0, 0, 1),
    ("04212345", "Quetiapine", "Seroquel", "N05AH04", "Antipsychotics", "Tablet", "PO", 0, 0, 1),
    ("04312345", "Lorazepam", "Ativan", "N05BA06", "Benzodiazepines", "Tablet", "PO", 1, 1, 1),
    ("04412345", "Levothyroxine", "Synthroid", "H03AA01", "Thyroid hormones", "Tablet", "PO", 0, 0, 1),
    ("04512345", "Potassium chloride", "K-Dur", "A12BA01", "Electrolytes", "Injection", "IV", 0, 1, 1),
    ("04612345", "Cisplatin", "Platinol", "L01XA01", "Antineoplastics", "Injection", "IV", 0, 1, 1),
    ("04712345", "Paclitaxel", "Taxol", "L01CD01", "Antineoplastics", "Injection", "IV", 0, 1, 1),
]

PAYERS = [
    ("PAY001", "Ontario Health Insurance Plan", "Public", "Standard", 0.62),
    ("PAY002", "Sun Life Financial", "Private", "Gold", 0.09),
    ("PAY003", "Manulife", "Private", "Gold", 0.08),
    ("PAY004", "Green Shield Canada", "Private", "Silver", 0.06),
    ("PAY005", "Canada Life", "Private", "Silver", 0.05),
    ("PAY006", "Blue Cross", "Private", "Bronze", 0.04),
    ("PAY007", "WSIB Ontario", "WSIB", "Standard", 0.03),
    ("PAY008", "Self Pay", "Self-pay", "None", 0.03),
]

# Payer-specific denial propensity — makes the claims report show real variation
PAYER_DENIAL_MULTIPLIER = {
    "PAY001": 0.55, "PAY002": 0.95, "PAY003": 1.05, "PAY004": 1.35,
    "PAY005": 1.20, "PAY006": 1.60, "PAY007": 0.80, "PAY008": 0.10,
}

DENIAL_REASONS = [
    ("CO-16", "Missing or invalid information", 0.24),
    ("CO-97", "Service included in another claim", 0.18),
    ("CO-50", "Not deemed medically necessary", 0.15),
    ("CO-29", "Time limit for filing expired", 0.11),
    ("CO-27", "Coverage terminated", 0.10),
    ("CO-18", "Duplicate claim", 0.09),
    ("CO-45", "Charge exceeds fee schedule", 0.08),
    ("CO-11", "Diagnosis inconsistent with procedure", 0.05),
]

SERVICE_CATEGORIES = [
    ("Room and board", 850, 2400),
    ("Surgical procedure", 1800, 14000),
    ("Diagnostic imaging", 220, 1900),
    ("Laboratory", 35, 480),
    ("Pharmacy", 25, 2200),
    ("Emergency services", 320, 1600),
    ("Physician services", 180, 1400),
    ("Rehabilitation therapy", 140, 620),
    ("Supplies", 40, 700),
]

APPOINTMENT_TYPES = ["New consultation", "Follow-up", "Procedure", "Diagnostic",
                     "Pre-operative", "Post-operative", "Annual review"]

# Deliberately non-standard source codes, to exercise ref_code_mapping
SEX_CODES = {
    "EHR": {"M": "M", "F": "F", "1": "M", "2": "F", "U": "X"},
    "SCHED": {"MALE": "M", "FEMALE": "F", "OTHER": "X"},
    "FIN": {"m": "M", "f": "F", "x": "X"},
}


# =====================================================================
# GENERATOR
# =====================================================================

class OHNGenerator:

    def __init__(self, n_patients: int, start: date, end: date,
                 out_dir: str, seed: int = SEED, bed_scale: float | None = None):
        self.rng = random.Random(seed)
        self.n_patients = n_patients
        self.start = start
        self.end = end
        self.out_dir = out_dir
        self.span_days = (end - start).days
        self.people: list[Person] = []
        self.source_records: list[SourceRecord] = []
        self.hospitals: list[dict] = []
        self.departments: list[dict] = []
        self.beds: list[dict] = []
        self.doctors: list[dict] = []
        self.tables: dict[str, tuple[list[str], list[list]]] = {}
        self.bed_scale = bed_scale if bed_scale is not None else self._auto_bed_scale()

    def _auto_bed_scale(self) -> float:
        """Size the bed inventory to the generated patient population.

        The hospitals in HOSPITALS are described at real-world scale — a
        420-bed acute hospital serves a catchment of roughly half a million
        people. Generating 3,000 patients against that inventory produces an
        occupancy rate under 1%, and every capacity report reads as an empty
        hospital. Beds are therefore scaled so that the activity this many
        patients actually generate lands near a realistic occupancy.

            beds = (admissions x ALOS) / (days x target occupancy)

        Admissions per patient and ALOS are the empirically observed values
        from this generator; both are checked by validate_data.py.
        """
        # Measured empirically from this generator, not assumed: 25,000
        # patients produce ~80,000 occupied bed-days over the default span
        # (11,800 admissions at a 6.4-day mean stay). An earlier figure taken
        # from a 3,000-patient run was almost half this, because at that scale
        # most departments had no beds at all and their stays were never
        # counted. Re-measure with validate_data.py if activity rates change.
        BED_DAYS_PER_PATIENT = 3.25
        TARGET_OCCUPANCY = 0.85

        bed_days_needed = self.n_patients * BED_DAYS_PER_PATIENT
        beds_needed = bed_days_needed / (self.span_days * TARGET_OCCUPANCY)

        default_total = sum(h[5] for h in HOSPITALS)
        scale = beds_needed / default_total

        # Floor: below roughly 9,000 patients the arithmetic asks for fewer
        # than 20 beds across the whole network, which leaves wards with one
        # or two beds each and makes ward-level occupancy reporting
        # meaningless. The floor keeps the ward structure intact at the cost
        # of a lower observed occupancy rate — the honest tradeoff at small
        # scale. Generate 25,000+ patients for occupancy near the target.
        return max(0.02, min(1.0, scale))

    # ------------------------------------------------------------- utils
    def rand_date(self) -> date:
        return self.start + timedelta(days=self.rng.randrange(self.span_days))

    def rand_datetime(self, d: date | None = None) -> datetime:
        d = d or self.rand_date()
        return datetime(d.year, d.month, d.day,
                        self.rng.randrange(24), self.rng.randrange(60),
                        self.rng.randrange(60))

    def chance(self, key: str) -> bool:
        return self.rng.random() < DEFECTS[key]

    def add_table(self, name: str, header: list[str], rows: list[list]):
        self.tables[name] = (header, rows)

    # ------------------------------------------------------- facilities
    def gen_facilities(self):
        hosp_rows = []
        for hid, name, ftype, region, city, beds, has_ed, teaching in HOSPITALS:
            beds = 0 if beds == 0 else max(6, int(round(beds * self.bed_scale)))
            self.hospitals.append({
                "hospital_id": hid, "name": name, "type": ftype,
                "region": region, "licensed_beds": beds, "has_ed": has_ed,
            })
            hosp_rows.append([hid, name, ftype, region, f"LHIN-{region.split()[-1][:1]}",
                              city, "ON", beds, has_ed, teaching,
                              fmt_ts(datetime(2024, 1, 1, 3, 0, 0))])
        self.add_table("FACIL/hospital",
                       ["hospital_id", "hospital_name", "facility_type", "region",
                        "health_region_code", "city", "province", "licensed_beds",
                        "has_emergency_dept", "is_teaching_hospital", "update_ts"],
                       hosp_rows)

        dept_rows = []
        for h in self.hospitals:
            for did, dname, sline, clinical, inpatient in DEPARTMENTS:
                # Clinics have no inpatient units; rehab hospitals are rehab-only
                if h["type"] == "Clinic" and inpatient:
                    continue
                if h["type"] == "Rehab" and dname not in (
                        "Rehabilitation", "Internal Medicine", "Laboratory", "Pharmacy"):
                    continue
                if dname == "Emergency" and not h["has_ed"]:
                    continue
                key = f"{h['hospital_id']}-{did}"
                self.departments.append({
                    "department_id": key, "name": dname, "service_line": sline,
                    "hospital_id": h["hospital_id"], "is_inpatient": inpatient,
                    "is_clinical": clinical,
                })
                dept_rows.append([key, dname, sline, h["hospital_id"],
                                  f"CC{abs(hash(key)) % 9000 + 1000}",
                                  clinical, inpatient,
                                  fmt_ts(datetime(2024, 1, 1, 3, 0, 0))])
        self.add_table("FACIL/department",
                       ["department_id", "department_name", "service_line", "hospital_id",
                        "cost_centre", "is_clinical", "is_inpatient_unit", "update_ts"],
                       dept_rows)

        bed_rows = []
        for h in self.hospitals:
            if h["licensed_beds"] == 0:
                continue
            inpatient_depts = [d for d in self.departments
                               if d["hospital_id"] == h["hospital_id"] and d["is_inpatient"]]
            if not inpatient_depts:
                continue
            # Distribute the hospital's bed budget across its inpatient units.
            # A per-department floor would multiply a small budget by the
            # department count and silently rebuild a full-size hospital.
            n_units = len(inpatient_depts)
            # Every inpatient unit needs at least one bed. A unit with zero
            # beds still admits patients, and those stays then contribute no
            # bed-days at all, which silently deflates the occupancy rate.
            budget = max(h["licensed_beds"], n_units)
            base_alloc = budget // n_units
            remainder = budget - base_alloc * n_units
            for unit_ix, d in enumerate(inpatient_depts):
                per_dept = base_alloc + (1 if unit_ix < remainder else 0)
                if per_dept < 1:
                    continue
                if d["name"] == "Rehabilitation":
                    ward_type = "Rehab"
                elif d["name"] == "Obstetrics":
                    ward_type = "Maternity"
                elif d["name"] in ("Internal Medicine", "Respirology", "Cardiology"):
                    ward_type = self.rng.choices(["Med-Surg", "ICU", "Step-Down"],
                                                 weights=[70, 18, 12])[0]
                else:
                    ward_type = self.rng.choices(["Med-Surg", "Step-Down"],
                                                 weights=[85, 15])[0]
                ward = f"{d['name'][:4].upper()}-{ward_type[:3].upper()}"
                for i in range(per_dept):
                    bed_id = f"{h['hospital_id']}-{d['department_id'][-3:]}-{i+1:03d}"
                    room = f"{(i // 2) + 100}"
                    bed_type = "ICU" if ward_type == "ICU" else self.rng.choice(
                        ["Standard", "Standard", "Standard", "Bariatric", "Telemetry"])
                    isolation = 1 if (ward_type in ("ICU", "Step-Down")
                                      or self.rng.random() < 0.15) else 0
                    self.beds.append({
                        "bed_id": bed_id, "hospital_id": h["hospital_id"],
                        "department_id": d["department_id"], "ward_type": ward_type,
                        "service_line": d["service_line"],
                    })
                    bed_rows.append([bed_id, room, ward, ward_type, bed_type,
                                     h["hospital_id"], d["department_id"], isolation, 1,
                                     fmt_ts(datetime(2024, 1, 1, 3, 0, 0))])
        self.add_table("FACIL/bed",
                       ["bed_id", "room_number", "ward_name", "ward_type", "bed_type",
                        "hospital_id", "department_id", "is_isolation_capable",
                        "is_active", "update_ts"],
                       bed_rows)

    # ---------------------------------------------------------- doctors
    def gen_doctors(self, n: int = 240):
        rows = []
        for i in range(n):
            did = f"DR{i+1:05d}"
            sex = self.rng.choice(["M", "F"])
            given = self.rng.choice(GIVEN_M if sex == "M" else GIVEN_F)
            family = self.rng.choice(FAMILY)
            specialty = self.rng.choice(SPECIALTIES)
            matching = [d for d in self.departments
                        if d["service_line"] == specialty or d["name"] == specialty]
            dept = self.rng.choice(matching) if matching else self.rng.choice(
                [d for d in self.departments if d["is_clinical"]])
            emp = self.rng.choices(["Staff", "Locum", "Resident"], weights=[76, 12, 12])[0]
            fte = round(self.rng.choices([1.0, 0.8, 0.6, 0.5, 0.4],
                                         weights=[52, 20, 12, 10, 6])[0], 2)
            hire = date(2024, 1, 1) - timedelta(days=self.rng.randrange(200, 8000))
            self.doctors.append({
                "doctor_id": did, "specialty": specialty,
                "department_id": dept["department_id"],
                "hospital_id": dept["hospital_id"], "fte": fte,
                "employment_type": emp,
            })
            rows.append([did, f"Dr. {given} {family}", given, family, specialty,
                         self.rng.choice(["", "Interventional", "Paediatric", "Surgical"]),
                         self.rng.choice(["MD", "MD FRCPC", "MD FRCSC", "MD CCFP"]),
                         dept["department_id"], dept["hospital_id"], emp, fte,
                         fmt_d(hire), 1, fmt_ts(datetime(2024, 1, 1, 4, 0, 0))])
        self.add_table("HR/doctor",
                       ["doctor_id", "display_name", "first_name", "last_name",
                        "specialty", "sub_specialty", "credential", "primary_department_id",
                        "hospital_id", "employment_type", "fte", "hire_date",
                        "is_active", "update_ts"],
                       rows)

    # ----------------------------------------------------------- people
    def gen_people(self):
        """Create the ground-truth population, then project each person into
        one or more source systems with realistic drift."""
        truth_rows = []

        for i in range(self.n_patients):
            sex = self.rng.choices(["M", "F", "X"], weights=[48.5, 51.0, 0.5])[0]
            given = self.rng.choice(GIVEN_M if sex == "M" else GIVEN_F)
            family = self.rng.choice(FAMILY)

            # Age distribution skewed toward older patients, who use more care
            age = int(min(99, max(0, self.rng.gauss(52, 22))))
            birth = date(2026, 1, 1) - timedelta(days=age * 365 + self.rng.randrange(365))

            fsa = self.rng.choice(FSA_POOL)
            person = Person(
                person_id=f"TRUE{i+1:07d}",
                given_name=given, family_name=family, birth_date=birth, sex=sex,
                hcn=make_hcn(self.rng), fsa=fsa,
                postal_code=make_postal(self.rng, fsa),
                prior_postal_code=make_postal(self.rng, self.rng.choice(FSA_POOL)),
                phone=make_phone(self.rng), prior_phone=make_phone(self.rng),
                language=self.rng.choice(LANGUAGES),
            )
            if age > 70 and self.rng.random() < 0.04:
                person.is_deceased = True
                person.deceased_date = self.rand_date()

            self.people.append(person)

            # --- Project into source systems --------------------------
            # Everyone is in the EHR. Most are in scheduling. Fewer in finance.
            systems = ["EHR"]
            if self.rng.random() < 0.82:
                systems.append("SCHED")
            if self.rng.random() < 0.64:
                systems.append("FIN")

            for sys_name in systems:
                rec = self._project(person, sys_name)
                self.source_records.append(rec)
                person.source_ids.setdefault(sys_name, []).append(rec.source_patient_id)

            # --- Intra-source duplicate -------------------------------
            # The classic problem: same person registered twice in the EHR,
            # usually at a different facility or after a name change.
            if self.chance("intra_source_duplicate"):
                dup = self._project(person, "EHR", force_drift=True)
                dup.is_injected_duplicate = True
                self.source_records.append(dup)
                person.source_ids["EHR"].append(dup.source_patient_id)

            n_source_records = sum(len(v) for v in person.source_ids.values())
            truth_rows.append([
                person.person_id, person.given_name, person.family_name,
                fmt_d(person.birth_date), person.sex, person.hcn,
                person.postal_code, n_source_records,
                "|".join(f"{k}:{v}" for k, vs in person.source_ids.items() for v in vs),
            ])

        self.add_table("_truth/patient_truth",
                       ["true_person_id", "given_name", "family_name", "birth_date",
                        "sex", "hcn", "postal_code", "source_record_count", "source_ids"],
                       truth_rows)

    def _project(self, p: Person, system: str, force_drift: bool = False) -> SourceRecord:
        """Render one person as one source system sees them."""
        rng = self.rng
        given, family = p.given_name, p.family_name

        # Nicknames and typos are more likely in secondary systems, where the
        # record was often typed from a phone call rather than a health card.
        drift_boost = 2.0 if (system != "EHR" or force_drift) else 1.0
        if rng.random() < DEFECTS["nickname_used"] * drift_boost and given in NICKNAMES:
            given = NICKNAMES[given]
        if rng.random() < DEFECTS["name_typo"] * drift_boost:
            given = typo(rng, given)
        if rng.random() < DEFECTS["name_typo"] * drift_boost * 0.6:
            family = typo(rng, family)

        # Health card
        hcn = p.hcn
        if rng.random() < DEFECTS["missing_hcn"] * (1.6 if system == "FIN" else 1.0):
            hcn = ""
        elif rng.random() < DEFECTS["invalid_hcn"]:
            hcn = make_hcn(rng, valid=False)
        # Format variance: finance stores it with dashes, scheduling with spaces
        if hcn:
            if system == "FIN" and rng.random() < 0.5:
                hcn = f"{hcn[:4]}-{hcn[4:7]}-{hcn[7:]}"
            elif system == "SCHED" and rng.random() < 0.3:
                hcn = f"{hcn[:4]} {hcn[4:7]} {hcn[7:]}"

        # Birth date — format varies by system, sometimes missing or impossible
        if rng.random() < DEFECTS["missing_dob"]:
            dob = ""
        elif rng.random() < DEFECTS["impossible_dob"]:
            bad = rng.choice([date(1823, 5, 1), date(2031, 4, 12), date(1899, 1, 1)])
            dob = fmt_d(bad)
        else:
            if system == "FIN":
                dob = p.birth_date.strftime("%d/%m/%Y")
            elif system == "SCHED" and rng.random() < 0.4:
                dob = p.birth_date.strftime("%m/%d/%Y")
            else:
                dob = fmt_d(p.birth_date)

        # Address — secondary systems often hold a stale one
        postal = p.postal_code
        if system != "EHR" and rng.random() < DEFECTS["stale_address"]:
            postal = p.prior_postal_code
        if rng.random() < DEFECTS["missing_postal"]:
            postal = ""
        elif rng.random() < DEFECTS["malformed_postal"]:
            postal = rng.choice([postal[:3], postal + "1", postal.replace(
                postal[0], "1", 1), "00000"])
        elif rng.random() < 0.45:
            postal = f"{postal[:3]} {postal[3:]}"   # spaced format
        elif rng.random() < 0.2:
            postal = postal.lower()

        phone = p.phone if rng.random() > 0.25 else p.prior_phone
        if phone:
            style = rng.random()
            if style < 0.3:
                phone = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
            elif style < 0.55:
                phone = f"{phone[:3]}-{phone[3:6]}-{phone[6:]}"
            elif style < 0.65:
                phone = ""

        # Sex code — each system uses its own vocabulary
        code_map = {v: k for k, v in SEX_CODES[system].items()}
        sex_code = code_map.get(p.sex, "U")
        if rng.random() < DEFECTS["unmapped_sex_code"]:
            sex_code = rng.choice(["9", "UNK", "?", "N"])

        prefix = {"EHR": "MRN", "SCHED": "SCH", "FIN": "ACC"}[system]
        pid = f"{prefix}{rng.randrange(10**7, 10**8)}"

        updated = self.rand_datetime()

        return SourceRecord(
            source_system=system, source_patient_id=pid, person_id=p.person_id,
            given_name=given, family_name=family, birth_date=dob, sex_code=sex_code,
            hcn=hcn, postal_code=postal, phone=phone,
            language=p.language if system == "EHR" else "",
            updated_ts=fmt_ts(updated),
            is_injected_duplicate=force_drift,
        )

    def write_patient_tables(self):
        ehr, sched, fin = [], [], []
        for r in self.source_records:
            if r.source_system == "EHR":
                ehr.append([r.source_patient_id, r.given_name, r.family_name,
                            r.birth_date, r.sex_code, r.hcn, r.postal_code,
                            r.phone, r.language,
                            "Y" if self._person(r).is_deceased else "N",
                            r.updated_ts])
            elif r.source_system == "SCHED":
                sched.append([r.source_patient_id, r.given_name, r.family_name,
                              r.birth_date, r.sex_code, r.hcn, r.postal_code,
                              r.phone, r.updated_ts])
            else:
                fin.append([r.source_patient_id, r.given_name, r.family_name,
                            r.birth_date, r.sex_code, r.hcn, r.postal_code,
                            r.phone, r.updated_ts])

        self.add_table("EHR/patient",
                       ["patient_id", "first_name", "last_name", "date_of_birth",
                        "gender", "health_card_number", "postal_code", "primary_phone",
                        "preferred_language", "deceased_indicator", "last_modified_ts"],
                       ehr)
        self.add_table("SCHED/patient",
                       ["patient_ref", "given", "surname", "dob", "sex", "health_card",
                        "postal", "contact_phone", "modified_at"],
                       sched)
        self.add_table("FIN/patient_account",
                       ["account_holder_id", "first_nm", "last_nm", "birth_dt",
                        "gender_cd", "hc_number", "zip_postal", "phone_number", "update_dt"],
                       fin)

    def _person(self, rec: SourceRecord) -> Person:
        idx = int(rec.person_id.replace("TRUE", "")) - 1
        return self.people[idx]

    # ------------------------------------------------------- reference
    def gen_reference(self):
        self.add_table("REF/icd10ca",
                       ["diagnosis_code", "diagnosis_description", "chapter",
                        "category", "is_chronic"],
                       [[c, d, ch, cat, chronic] for c, d, ch, cat, chronic in DIAGNOSES])

        self.add_table("REF/loinc",
                       ["loinc_code", "test_name", "panel_name", "specimen_type",
                        "result_unit", "reference_low", "reference_high", "is_stat_capable"],
                       [[c, n, p, s, u if u else "", lo if lo is not None else "",
                         hi if hi is not None else "", st]
                        for c, n, p, s, u, lo, hi, st in LAB_TESTS])

        self.add_table("REF/medication",
                       ["din", "generic_name", "brand_name", "atc_code", "atc_class",
                        "dosage_form", "route", "is_controlled_substance",
                        "is_high_alert", "is_formulary"],
                       [list(m) for m in MEDICATIONS])

        self.add_table("REF/payer",
                       ["payer_id", "payer_name", "payer_type", "plan_tier"],
                       [[p[0], p[1], p[2], p[3]] for p in PAYERS])

        mapping = []
        for system, codes in SEX_CODES.items():
            for src, std in codes.items():
                desc = {"M": "Male", "F": "Female", "X": "Other or undifferentiated"}[std]
                mapping.append(["SEX", system, src, std, desc, "true"])
        for code, desc, emerg, elect in [
            ("E", "Emergency", "true", "false"), ("U", "Urgent", "false", "false"),
            ("EL", "Elective", "false", "true"), ("N", "Newborn", "false", "false"),
            ("T", "Transfer", "false", "false")]:
            std = {"E": "EMERGENCY", "U": "URGENT", "EL": "ELECTIVE",
                   "N": "NEWBORN", "T": "TRANSFER"}[code]
            mapping.append(["ADMIT_TYPE", "EHR", code, std, desc, "true"])
        for code, std, desc in [
            ("A1", "APPROVED", "Approved in full"), ("A2", "PARTIAL", "Partially approved"),
            ("D1", "DENIED", "Denied"), ("P1", "PENDING", "Pending adjudication"),
            ("S1", "SUBMITTED", "Submitted"), ("PD", "PAID", "Paid"),
            ("AP", "APPEALED", "Under appeal"), ("V1", "VOID", "Voided")]:
            mapping.append(["CLAIM_STATUS", "CLAIMS", code, std, desc, "true"])
        for code, std, desc in [
            ("HOME", "HOME", "Discharged home"), ("HMCR", "HOME_CARE", "Home with services"),
            ("TRAN", "TRANSFER", "Transferred to another facility"),
            ("LTC", "LTC", "Long-term care"), ("AMA", "AMA", "Left against medical advice"),
            ("EXP", "EXPIRED", "Died in hospital"), ("REHB", "REHAB", "Transferred to rehab")]:
            mapping.append(["DISPOSITION", "EHR", code, std, desc, "true"])
        self.add_table("REF/code_mapping",
                       ["domain", "source_system", "source_code", "standard_code",
                        "standard_description", "is_active"],
                       mapping)

    # =================================================================
    # ACTIVITY GENERATION
    #
    # Correlations are planted deliberately. If every measure is random
    # noise, every report page shows a flat line and the platform looks
    # broken. Each correlation below is something a report is meant to
    # reveal — patients with longer ED waits report lower satisfaction,
    # readmission risk rises with age and comorbidity, no-shows rise with
    # booking lead time.
    # =================================================================

    def gen_activity(self):
        rng = self.rng
        ed_depts = [d for d in self.departments if d["name"] == "Emergency"]
        clinic_depts = [d for d in self.departments if d["is_clinical"]
                        and d["name"] not in ("Emergency", "Laboratory", "Pharmacy")]
        inpatient_depts = [d for d in self.departments if d["is_inpatient"]]
        beds_by_dept: dict[str, list] = {}
        for b in self.beds:
            beds_by_dept.setdefault(b["department_id"], []).append(b)
        docs_by_dept: dict[str, list] = {}
        for d in self.doctors:
            docs_by_dept.setdefault(d["department_id"], []).append(d)

        payer_ids = [p[0] for p in PAYERS]
        payer_weights = [p[4] for p in PAYERS]

        appt_rows, appt_hist_rows = [], []
        adm_rows, bed_rows, diag_rows = [], [], []
        ed_rows = []
        lab_rows = []
        med_rows = []
        claim_hdr_rows, claim_line_rows = [], []
        inv_rows, inv_line_rows = [], []
        survey_rows = []

        seq = {"appt": 0, "adm": 0, "ed": 0, "lab": 0, "med": 0,
               "clm": 0, "inv": 0, "srv": 0, "enc": 0}

        def nid(kind: str, prefix: str) -> str:
            seq[kind] += 1
            return f"{prefix}{seq[kind]:08d}"

        # Per-person state that drives realistic correlations
        no_show_propensity = {}
        for p in self.people:
            age = (date(2026, 1, 1) - p.birth_date).days // 365
            base = BASE_NO_SHOW_RATE
            if age < 30:
                base *= 1.7
            elif age < 45:
                base *= 1.25
            elif age > 70:
                base *= 0.65
            no_show_propensity[p.person_id] = min(0.55, base * rng.uniform(0.4, 2.2))

        # ------------------------------------------------ patient loop
        for p in self.people:
            age = (date(2026, 1, 1) - p.birth_date).days // 365
            payer = rng.choices(payer_ids, weights=payer_weights, k=1)[0]

            # Utilisation rises with age; a small high-utiliser tail dominates
            intensity = max(0.2, rng.lognormvariate(0.0, 0.85)) * (0.6 + age / 90)

            ehr_id = p.source_ids["EHR"][0]
            sched_id = p.source_ids.get("SCHED", [ehr_id])[0]

            # ---------------------------------------- appointments
            n_appt = int(rng.gauss(6, 3) * intensity)
            n_appt = max(0, min(45, n_appt))
            prior_no_show = 0

            for _ in range(n_appt):
                dept = rng.choice(clinic_depts)
                docs = docs_by_dept.get(dept["department_id"])
                if not docs:
                    continue
                doc = rng.choice(docs)

                sched_day = self.rand_date()
                if sched_day.weekday() >= 5 and rng.random() < 0.85:
                    sched_day += timedelta(days=(7 - sched_day.weekday()))
                if sched_day > self.end:
                    continue

                lead_days = int(max(0, rng.lognormvariate(2.6, 0.9)))
                booking_day = sched_day - timedelta(days=lead_days)
                hour, minute = business_hour(rng)
                sched_ts = datetime(sched_day.year, sched_day.month, sched_day.day,
                                    hour, minute)
                booking_ts = datetime(booking_day.year, booking_day.month,
                                      booking_day.day, *business_hour(rng))

                appt_id = nid("appt", "APT")
                appt_type = rng.choice(APPOINTMENT_TYPES)
                duration = rng.choice([15, 15, 20, 30, 30, 45, 60])
                is_virtual = 1 if rng.random() < 0.18 else 0
                is_first = 1 if appt_type == "New consultation" else 0

                # No-show risk rises with lead time and with prior no-shows.
                # The multipliers are kept modest and capped: they compound,
                # and unbounded compounding produced a 27% network-wide
                # no-show rate, roughly triple what a real clinic sees.
                ns_risk = no_show_propensity[p.person_id]
                ns_risk *= 1.0 + min(0.55, lead_days / 110.0)
                ns_risk *= 1.0 + 0.18 * min(3, prior_no_show)
                if is_virtual:
                    ns_risk *= 0.55
                ns_risk = min(ns_risk, 0.42)

                cancel_risk = BASE_CANCELLATION_RATE * (1.0 + lead_days / 90.0)

                roll = rng.random()
                cancel_ts = ""
                checkin_ts = seen_ts = checkout_ts = ""
                cancelled_by = ""

                if roll < cancel_risk:
                    status = rng.choices(
                        ["CANCELLED_PATIENT", "CANCELLED_PROVIDER", "CANCELLED_FACILITY"],
                        weights=[68, 24, 8])[0]
                    cancelled_by = status.split("_")[1].capitalize()
                    notice_h = max(0.5, rng.lognormvariate(3.1, 1.1))
                    cancel_ts = fmt_ts(sched_ts - timedelta(hours=notice_h))
                elif roll < cancel_risk + ns_risk:
                    status = "NO_SHOW"
                    prior_no_show += 1
                elif sched_ts > datetime.combine(self.end, datetime.min.time()):
                    status = "SCHEDULED"
                else:
                    status = "COMPLETED"
                    wait = max(0, int(rng.lognormvariate(2.7, 0.75)))
                    checkin = sched_ts - timedelta(minutes=rng.randrange(0, 20))
                    seen = checkin + timedelta(minutes=wait)
                    actual = max(5, int(rng.gauss(duration, duration * 0.3)))
                    checkin_ts = fmt_ts(checkin)
                    seen_ts = fmt_ts(seen)
                    checkout_ts = fmt_ts(seen + timedelta(minutes=actual))

                appt_rows.append([
                    appt_id, sched_id, doc["doctor_id"], dept["department_id"],
                    dept["hospital_id"], fmt_ts(booking_ts), fmt_ts(sched_ts),
                    duration, appt_type, is_first, is_virtual,
                    checkin_ts, seen_ts, checkout_ts, cancel_ts, cancelled_by,
                    status, fmt_ts(sched_ts + timedelta(days=1)),
                ])
                appt_hist_rows.append([appt_id, "BOOKED", fmt_ts(booking_ts)])
                if status != "BOOKED":
                    stat_ts = cancel_ts or checkout_ts or fmt_ts(sched_ts)
                    appt_hist_rows.append([appt_id, status, stat_ts])

            # ---------------------------------------- ED visits
            ed_risk = 0.30 * intensity
            n_ed = sum(1 for _ in range(6) if rng.random() < ed_risk / 3)
            ed_visit_ids = []

            for _ in range(n_ed):
                if not ed_depts:
                    break
                dept = rng.choice(ed_depts)
                d = self.rand_date()
                arrival = datetime(d.year, d.month, d.day, diurnal_hour(rng),
                                   rng.randrange(60))

                ctas = weighted_choice(rng, CTAS_DISTRIBUTION)
                if age > 70:
                    ctas = max(1, ctas - (1 if rng.random() < 0.35 else 0))
                elif age < 18:
                    ctas = min(5, ctas + (1 if rng.random() < 0.25 else 0))

                # Crowding: evenings and Mondays are worse
                crowding = 1.0
                if arrival.hour in range(16, 23):
                    crowding *= 1.35
                if arrival.weekday() == 0:
                    crowding *= 1.18
                if arrival.weekday() >= 5:
                    crowding *= 1.10

                triage_wait = max(1, rng.lognormvariate(math.log(8 * crowding), 0.6))
                pia_median = CTAS_PIA_MEDIAN[ctas] * crowding
                pia = max(triage_wait + 1,
                          rng.lognormvariate(math.log(pia_median), 0.62))

                arrival_mode = rng.choices(["Walk-in", "Ambulance", "Transfer"],
                                           weights=[72, 25, 3])[0]
                if ctas <= 2:
                    arrival_mode = rng.choices(["Ambulance", "Walk-in", "Transfer"],
                                               weights=[68, 27, 5])[0]

                lwbs = rng.random() < min(
                    0.18, BASE_LWBS_RATE * (1.0 + (ctas - 1) * 0.28) * crowding)
                admit_prob = {1: 0.72, 2: 0.44, 3: 0.19, 4: 0.06, 5: 0.02}[ctas]
                admitted = (not lwbs) and rng.random() < admit_prob

                triage_ts = arrival + timedelta(minutes=triage_wait)
                if lwbs:
                    physician_ts = None
                    departure = arrival + timedelta(minutes=pia * rng.uniform(0.6, 1.4))
                    decision_ts = None
                    bed_ts = None
                else:
                    physician_ts = arrival + timedelta(minutes=pia)
                    workup = max(20, rng.lognormvariate(math.log(120 * crowding), 0.7))
                    decision_ts = physician_ts + timedelta(minutes=workup)
                    if admitted:
                        boarding = max(10, rng.lognormvariate(math.log(150 * crowding), 0.85))
                        bed_ts = decision_ts + timedelta(minutes=boarding)
                        departure = bed_ts
                    else:
                        bed_ts = None
                        departure = decision_ts + timedelta(minutes=rng.randrange(10, 90))

                # Injected defects
                if self.chance("impossible_ctas"):
                    ctas_out = rng.choice([0, 6, 9, ""])
                else:
                    ctas_out = ctas
                if self.chance("clock_error_ed"):
                    triage_ts = arrival - timedelta(minutes=rng.randrange(5, 90))

                ed_id = nid("ed", "EDV")
                enc_id = nid("enc", "ENC")
                ed_visit_ids.append((ed_id, enc_id, admitted, departure,
                                     pia if not lwbs else None, dept))

                dx = rng.choice([d0 for d0 in DIAGNOSES
                                 if d0[0].startswith(("R", "S", "J", "K", "N", "A"))])

                ed_rows.append([
                    ed_id, enc_id, ehr_id, dept["hospital_id"], dept["department_id"],
                    fmt_ts(arrival), fmt_ts(triage_ts),
                    fmt_ts(physician_ts) if physician_ts else "",
                    fmt_ts(decision_ts) if decision_ts else "",
                    fmt_ts(bed_ts) if bed_ts else "",
                    fmt_ts(departure), ctas_out, arrival_mode, dx[0],
                    1 if lwbs else 0, 1 if admitted else 0, payer,
                    fmt_ts(departure + timedelta(hours=2)),
                ])

            # ---------------------------------------- admissions
            n_adm = 0
            elective_adm = int(rng.random() < 0.14 * intensity)
            ed_admissions = [e for e in ed_visit_ids if e[2]]

            # Admissions are processed from a work queue rather than a fixed
            # list, because a discharge can generate a readmission that must
            # itself be processed. Without this, readmissions only occurred by
            # coincidence when two independent admissions happened to land
            # within 30 days, giving a network readmission rate near 1% —
            # about a tenth of the real figure.
            queue: list[tuple] = [("EMERGENCY", e, None) for e in ed_admissions]
            for _ in range(elective_adm):
                queue.append(("ELECTIVE", None, None))
            queue.sort(key=lambda x: x[1][3] if x[1] else datetime.min)

            prior_discharge = None
            prior_disposition = None
            qi = 0

            while qi < len(queue) and qi < 60:
                adm_type, ed_link, forced_ts = queue[qi]
                qi += 1

                if forced_ts is not None:
                    adm_ts = forced_ts
                    dept_pool = inpatient_depts
                    enc_id = nid("enc", "ENC")
                elif ed_link:
                    adm_ts = ed_link[3]
                    dept_pool = [d for d in inpatient_depts
                                 if d["hospital_id"] == ed_link[5]["hospital_id"]]
                    enc_id = ed_link[1]
                else:
                    adm_ts = self.rand_datetime()
                    dept_pool = inpatient_depts
                    enc_id = nid("enc", "ENC")
                if not dept_pool or adm_ts.date() > self.end:
                    continue
                dept = rng.choice(dept_pool)

                # Readmission: elevated when a recent discharge exists
                is_readmit_candidate = False
                if prior_discharge and (adm_ts - prior_discharge).days <= 30:
                    is_readmit_candidate = True

                sline = dept["service_line"]
                mu, sigma = LOS_PARAMS.get(sline, (1.5, 0.65))
                if age > 75:
                    mu += 0.22
                if is_readmit_candidate:
                    mu += 0.12
                los = lognormal_days(rng, mu, sigma)

                discharge_ts = adm_ts + timedelta(
                    days=los, hours=rng.randrange(-6, 8), minutes=rng.randrange(60))
                still_open = discharge_ts.date() > self.end
                if still_open:
                    discharge_ts = None

                # Disposition depends on age and service line
                if discharge_ts:
                    if age > 80 and rng.random() < 0.05:
                        disp = "EXP"
                    elif age > 75 and rng.random() < 0.14:
                        disp = rng.choice(["LTC", "REHB", "HMCR"])
                    elif rng.random() < 0.03:
                        disp = "TRAN"
                    elif rng.random() < 0.015:
                        disp = "AMA"
                    else:
                        disp = rng.choices(["HOME", "HMCR", "REHB"],
                                           weights=[80, 14, 6])[0]
                else:
                    disp = ""

                if discharge_ts and self.chance("clock_error_discharge"):
                    discharge_ts = adm_ts - timedelta(days=rng.randrange(1, 5))

                adm_id = nid("adm", "ADM")
                doc_pool = docs_by_dept.get(dept["department_id"]) or self.doctors
                doc = rng.choice(doc_pool)

                dx_pool = [d0 for d0 in DIAGNOSES]
                if sline == "Cardiology":
                    dx_pool = [d0 for d0 in DIAGNOSES if d0[2] == "Circulatory system"]
                elif sline == "Respirology":
                    dx_pool = [d0 for d0 in DIAGNOSES if d0[2] == "Respiratory system"]
                elif sline == "Oncology":
                    dx_pool = [d0 for d0 in DIAGNOSES if d0[2] == "Neoplasms"]
                elif sline == "Obstetrics":
                    dx_pool = [d0 for d0 in DIAGNOSES if d0[2] == "Pregnancy and childbirth"]
                elif sline == "Orthopaedics":
                    dx_pool = [d0 for d0 in DIAGNOSES if d0[2] in ("Musculoskeletal", "Injury")]
                primary_dx = rng.choice(dx_pool)

                expected_dis = adm_ts + timedelta(days=max(1, int(rng.gauss(los, 1.5))))

                adm_rows.append([
                    adm_id, enc_id, ehr_id, doc["doctor_id"], dept["department_id"],
                    dept["hospital_id"], fmt_ts(adm_ts),
                    fmt_ts(discharge_ts) if discharge_ts else "",
                    fmt_ts(expected_dis),
                    "E" if adm_type == "EMERGENCY" else "EL",
                    disp, primary_dx[0], payer,
                    fmt_ts((discharge_ts or adm_ts) + timedelta(hours=3)),
                ])

                # Diagnoses: one primary plus comorbidities
                diag_rows.append([enc_id, ehr_id, primary_dx[0], 1, "Discharge", 1, 1,
                                  fmt_d(adm_ts.date())])
                n_comorbid = rng.choices([0, 1, 2, 3, 4, 5],
                                         weights=[18, 26, 24, 16, 10, 6])[0]
                if age > 70:
                    n_comorbid += rng.randint(0, 2)
                chronic = [d0 for d0 in DIAGNOSES if d0[4] == 1 and d0[0] != primary_dx[0]]
                for rank, cd in enumerate(rng.sample(chronic, min(n_comorbid, len(chronic))), 2):
                    diag_rows.append([enc_id, ehr_id, cd[0], rank, "Comorbid", 0,
                                      1 if rng.random() < 0.8 else 0, fmt_d(adm_ts.date())])

                # Bed assignments — one per unit, more if transferred
                pool = beds_by_dept.get(dept["department_id"], [])
                if pool and discharge_ts:
                    n_moves = rng.choices([1, 2, 3], weights=[76, 19, 5])[0]
                    cursor = adm_ts
                    total = (discharge_ts - adm_ts).total_seconds()
                    if total > 0:
                        for m in range(n_moves):
                            bed = rng.choice(pool)
                            if m == n_moves - 1:
                                end = discharge_ts
                            else:
                                frac = rng.uniform(0.2, 0.7)
                                end = cursor + timedelta(seconds=total * frac / n_moves)
                            bed_rows.append([adm_id, enc_id, bed["bed_id"],
                                             fmt_ts(cursor), fmt_ts(end),
                                             "Transfer" if m > 0 else "Admission"])
                            cursor = end

                if discharge_ts:
                    prior_discharge = discharge_ts
                    prior_disposition = disp
                n_adm += 1

                # ------------------------------ enqueue a readmission
                # Risk factors mirror the ones the report is meant to surface:
                # age, chronic primary diagnosis, long index stay, and
                # discharge to a lower level of care. Patients who died or
                # were transferred out are not eligible.
                if discharge_ts and disp not in ("EXP", "TRAN") and n_adm < 12:
                    risk = BASE_READMISSION_RATE
                    if age > 75:
                        risk *= 1.55
                    elif age > 65:
                        risk *= 1.25
                    if primary_dx[4] == 1:          # chronic condition
                        risk *= 1.40
                    if los > 10:
                        risk *= 1.30
                    if disp in ("HMCR", "LTC", "AMA"):
                        risk *= 1.35
                    if adm_type == "ELECTIVE":
                        risk *= 0.35

                    if rng.random() < min(0.55, risk):
                        gap_days = int(min(30, max(1, rng.lognormvariate(2.35, 0.75))))
                        readmit_ts = discharge_ts + timedelta(
                            days=gap_days, hours=rng.randrange(0, 24))
                        if readmit_ts.date() <= self.end:
                            queue.append(("EMERGENCY", None, readmit_ts))

                # ------------------------------ labs for this admission
                n_lab = int(max(1, rng.gauss(4 + los * 1.6, 3)))
                for _ in range(min(n_lab, 40)):
                    test = rng.choice(LAB_TESTS)
                    order_ts = adm_ts + timedelta(
                        hours=rng.uniform(0, max(1, los * 24)))
                    priority = rng.choices(["Routine", "STAT"], weights=[78, 22])[0]
                    tat_median = 42 if priority == "STAT" else 210
                    collect_delay = max(2, rng.lognormvariate(math.log(20), 0.8))
                    result_delay = max(5, rng.lognormvariate(math.log(tat_median), 0.65))
                    collect_ts = order_ts + timedelta(minutes=collect_delay)
                    result_ts = collect_ts + timedelta(minutes=result_delay)

                    if self.chance("negative_lab_turnaround"):
                        result_ts = order_ts - timedelta(minutes=rng.randrange(10, 200))

                    loinc = test[0]
                    if self.chance("unmapped_loinc"):
                        loinc = rng.choice(["", "LOCAL-99", "XX-000", "PENDING"])

                    lo, hi = test[5], test[6]
                    value_txt = ""
                    value_num = ""
                    flag = "N"
                    if lo is not None:
                        span = hi - lo
                        abnormal = rng.random() < (0.34 if age > 65 else 0.22)
                        if abnormal:
                            direction = rng.choice([-1, 1])
                            magnitude = rng.uniform(0.15, 1.4)
                            value = (lo - span * magnitude) if direction < 0 else (hi + span * magnitude)
                            flag = "L" if direction < 0 else "H"
                            if magnitude > 0.9:
                                flag = flag * 2
                        else:
                            value = rng.uniform(lo, hi)
                        value_num = round(max(0, value), 3)
                    else:
                        value_txt = rng.choices(
                            ["No growth", "Normal flora", "E. coli isolated",
                             "S. aureus isolated", "Pending"],
                            weights=[62, 18, 9, 6, 5])[0]
                        flag = "H" if "isolated" in value_txt else "N"

                    lab_rows.append([
                        nid("lab", "LAB"), enc_id, ehr_id, doc["doctor_id"],
                        dept["department_id"], dept["hospital_id"], loinc,
                        fmt_ts(order_ts), fmt_ts(collect_ts), fmt_ts(result_ts),
                        priority, value_num, value_txt, flag,
                        1 if flag in ("HH", "LL") else 0,
                        fmt_ts(result_ts + timedelta(minutes=5)),
                    ])

                # ------------------------------ medications
                n_med = int(max(1, rng.gauss(3 + los * 0.9, 2)))
                for _ in range(min(n_med, 25)):
                    med = rng.choice(MEDICATIONS)
                    order_ts = adm_ts + timedelta(hours=rng.uniform(0, max(1, los * 24)))
                    dispense_delay = max(3, rng.lognormvariate(math.log(35), 0.8))
                    dispense_ts = order_ts + timedelta(minutes=dispense_delay)
                    din = med[0]
                    if self.chance("unmapped_din"):
                        din = rng.choice(["", "00000000", "LOCAL01"])
                    qty = rng.choice([1, 1, 2, 3, 5, 10, 14, 20, 30])
                    unit_cost = round(rng.lognormvariate(0.8, 1.3), 4)
                    med_rows.append([
                        nid("med", "MED"), enc_id, ehr_id, doc["doctor_id"],
                        dept["department_id"], dept["hospital_id"], din,
                        fmt_ts(order_ts), fmt_ts(dispense_ts),
                        round(rng.choice([5, 10, 20, 25, 40, 50, 100, 250, 500]), 2),
                        rng.choice(["mg", "mg", "mcg", "g", "mL", "units"]),
                        rng.choice(["OD", "BID", "TID", "QID", "Q4H", "Q6H", "PRN"]),
                        qty, qty if rng.random() > 0.06 else max(0, qty - rng.randint(1, 2)),
                        rng.choice([1, 3, 5, 7, 14, 30]),
                        unit_cost, round(unit_cost * qty, 2),
                        1 if rng.random() < 0.04 else 0,
                        fmt_ts(dispense_ts + timedelta(minutes=10)),
                    ])

                # ------------------------------ claim and invoice
                base_charge = 0.0
                inv_id = nid("inv", "INV")
                n_lines = rng.randint(2, 7)
                inv_line_rows_local = []
                for ln in range(1, n_lines + 1):
                    cat, lo_amt, hi_amt = rng.choice(SERVICE_CATEGORIES)
                    charge = round(rng.uniform(lo_amt, hi_amt) * (1 + los * 0.08), 2)
                    discount = round(charge * rng.choice([0, 0, 0, 0.05, 0.1]), 2)
                    tax = 0.0
                    net = round(charge - discount + tax, 2)
                    if self.chance("billing_line_imbalance"):
                        net = round(net + rng.uniform(1, 90), 2)
                    payment = round(net * rng.choice([0, 0.4, 0.75, 1.0, 1.0]), 2)
                    base_charge += net
                    inv_line_rows_local.append([
                        inv_id, ln, ehr_id, dept["department_id"], dept["hospital_id"],
                        payer, enc_id, f"SC{rng.randrange(1000, 9999)}", cat,
                        rng.choice([1, 1, 1, 2, 3]), charge, discount, tax, net,
                        payment, round(max(0, net - payment), 2),
                    ])
                inv_line_rows.extend(inv_line_rows_local)
                inv_rows.append([
                    inv_id, ehr_id, enc_id, dept["hospital_id"], payer,
                    fmt_d((discharge_ts or adm_ts).date()),
                    fmt_d(adm_ts.date()), round(base_charge, 2),
                    fmt_ts((discharge_ts or adm_ts) + timedelta(days=1)),
                ])

                if payer != "PAY008":
                    clm_id = nid("clm", "CLM")
                    submit = (discharge_ts or adm_ts) + timedelta(
                        days=rng.randrange(1, 14))
                    adjud_days = int(max(1, rng.lognormvariate(math.log(18), 0.75)))
                    adjud = submit + timedelta(days=adjud_days)
                    is_adjudicated = adjud.date() <= self.end

                    denial_rate = BASE_DENIAL_RATE * PAYER_DENIAL_MULTIPLIER[payer]
                    if base_charge > 12000:
                        denial_rate *= 1.4

                    billed = round(base_charge, 2)
                    if not is_adjudicated:
                        status, approved, paid, denied, reason = "P1", "", "", "", ""
                        pay_date = ""
                    else:
                        r = rng.random()
                        if r < denial_rate:
                            status = "D1"
                            approved, paid = 0.0, 0.0
                            denied = billed
                            reason = rng.choices([d[0] for d in DENIAL_REASONS],
                                                 weights=[d[2] for d in DENIAL_REASONS])[0]
                            pay_date = ""
                        elif r < denial_rate + 0.14:
                            status = "A2"
                            approved = round(billed * rng.uniform(0.45, 0.9), 2)
                            denied = round(billed - approved, 2)
                            paid = approved
                            reason = rng.choices([d[0] for d in DENIAL_REASONS],
                                                 weights=[d[2] for d in DENIAL_REASONS])[0]
                            pay_date = fmt_d((adjud + timedelta(days=rng.randrange(3, 30))).date())
                        else:
                            status = "A1"
                            approved = round(billed * rng.uniform(0.88, 1.0), 2)
                            denied = round(billed - approved, 2)
                            paid = approved
                            reason = ""
                            pay_date = fmt_d((adjud + timedelta(days=rng.randrange(3, 30))).date())

                        if self.chance("claim_over_approval") and approved != "":
                            approved = round(billed * rng.uniform(1.05, 1.4), 2)

                    claim_hdr_rows.append([
                        clm_id, ehr_id, doc["doctor_id"], dept["department_id"],
                        dept["hospital_id"], payer, enc_id, primary_dx[0],
                        fmt_d(adm_ts.date()), fmt_d(submit.date()),
                        fmt_d(adjud.date()) if is_adjudicated else "",
                        pay_date, status, billed,
                        round(billed * 0.95, 2) if is_adjudicated else "",
                        approved, paid, denied, reason,
                        round(float(paid) * 0.08, 2) if paid not in ("", 0.0) else 0.0,
                        "", 0,
                        fmt_ts(adjud + timedelta(hours=6)),
                    ])
                    for ln, line in enumerate(inv_line_rows_local, 1):
                        claim_line_rows.append([clm_id, ln, line[7], line[8],
                                                line[9], line[13]])

                # ------------------------------ satisfaction survey
                # Response rate is deliberately non-random: patients with very
                # good and very bad experiences respond more often than those
                # in the middle, which is the response bias every real patient
                # experience programme has to reckon with.
                if discharge_ts and rng.random() < 0.42:
                    ed_wait = None
                    for e in ed_visit_ids:
                        if e[1] == enc_id and e[4]:
                            ed_wait = e[4]
                    # Satisfaction falls as waits rise and as LOS extends
                    base_score = 8.4
                    if ed_wait:
                        base_score -= min(3.2, ed_wait / 60.0 * 0.55)
                    base_score -= min(1.5, los * 0.06)
                    if disp in ("AMA", "EXP"):
                        base_score -= 2.0

                    def score(offset=0.0):
                        v = rng.gauss(base_score + offset, 1.35)
                        return max(1, min(10, int(round(v))))

                    overall = score()
                    if self.chance("survey_score_out_of_range"):
                        overall = rng.choice([0, 11, 99, -1])
                    nps = ("Promoter" if overall >= 9 else
                           "Passive" if overall >= 7 else "Detractor")
                    survey_rows.append([
                        nid("srv", "SRV"), ehr_id, doc["doctor_id"],
                        dept["department_id"], dept["hospital_id"], enc_id,
                        fmt_d((discharge_ts + timedelta(days=rng.randrange(2, 21))).date()),
                        fmt_d(discharge_ts.date()), "Inpatient",
                        overall, score(-0.9), score(0.6), score(0.3), score(0.1),
                        score(-0.3), score(0.2), nps,
                        1 if rng.random() < 0.42 else 0,
                    ])

            # ------------------------------ ED-only surveys
            # ED visits that did not lead to admission are surveyed too. These
            # carry the strongest wait-time signal, because the wait is
            # essentially the whole experience.
            for ed_id, enc_id_ed, admitted_ed, dep_ts, pia_min, ed_dept in ed_visit_ids:
                if admitted_ed or pia_min is None or rng.random() > 0.20:
                    continue
                base_score = 8.6 - min(4.0, pia_min / 60.0 * 0.85)

                def ed_score(offset=0.0, _b=base_score):
                    return max(1, min(10, int(round(rng.gauss(_b + offset, 1.4)))))

                overall = ed_score()
                nps = ("Promoter" if overall >= 9 else
                       "Passive" if overall >= 7 else "Detractor")
                ed_doc = rng.choice(docs_by_dept.get(ed_dept["department_id"]) or self.doctors)
                survey_rows.append([
                    nid("srv", "SRV"), ehr_id, ed_doc["doctor_id"],
                    ed_dept["department_id"], ed_dept["hospital_id"], enc_id_ed,
                    fmt_d((dep_ts + timedelta(days=rng.randrange(1, 14))).date()),
                    fmt_d(dep_ts.date()), "Emergency",
                    overall, ed_score(-1.6), ed_score(0.5), ed_score(0.2),
                    ed_score(0.0), ed_score(-0.5), ed_score(0.1), nps,
                    1 if rng.random() < 0.48 else 0,
                ])

        # ------------------------------------------------ register tables
        self.add_table("SCHED/appointment",
                       ["appointment_id", "patient_ref", "doctor_id", "department_id",
                        "hospital_id", "booking_ts", "scheduled_ts", "scheduled_duration_min",
                        "appointment_type", "is_first_visit", "is_virtual", "checkin_ts",
                        "seen_ts", "checkout_ts", "cancellation_ts", "cancelled_by",
                        "status_code", "modified_at"],
                       appt_rows)
        self.add_table("SCHED/appointment_status_history",
                       ["appointment_id", "status_code", "status_ts"], appt_hist_rows)
        self.add_table("EHR/admission",
                       ["admission_id", "encounter_id", "patient_id", "attending_physician_id",
                        "department_id", "hospital_id", "admission_ts", "discharge_ts",
                        "expected_discharge_ts", "admission_type", "discharge_disposition_code",
                        "primary_diagnosis_code", "payer_id", "last_modified_ts"],
                       adm_rows)
        self.add_table("EHR/bed_assignment",
                       ["admission_id", "encounter_id", "bed_id", "assignment_start_ts",
                        "assignment_end_ts", "assignment_reason"], bed_rows)
        self.add_table("EHR/diagnosis",
                       ["encounter_id", "patient_id", "diagnosis_code", "diagnosis_rank",
                        "diagnosis_type", "is_primary", "is_present_on_admission",
                        "diagnosis_date"], diag_rows)
        self.add_table("EHR/emergency_visit",
                       ["ed_visit_id", "encounter_id", "patient_id", "hospital_id",
                        "department_id", "arrival_ts", "triage_ts", "physician_seen_ts",
                        "admit_decision_ts", "bed_assigned_ts", "departure_ts",
                        "triage_score", "arrival_mode", "triage_diagnosis_code",
                        "left_without_being_seen", "resulted_in_admission", "payer_id",
                        "last_modified_ts"],
                       ed_rows)
        self.add_table("LIS/lab_result",
                       ["lab_result_id", "encounter_id", "patient_id", "ordering_doctor_id",
                        "department_id", "hospital_id", "loinc_code", "order_ts",
                        "collect_ts", "result_ts", "priority", "result_value_numeric",
                        "result_value_text", "abnormal_flag", "is_critical", "modified_at"],
                       lab_rows)
        self.add_table("PHARM/medication_order",
                       ["medication_order_id", "encounter_id", "patient_id",
                        "prescribing_doctor_id", "department_id", "hospital_id", "din",
                        "order_ts", "dispense_ts", "dose_amount", "dose_unit",
                        "frequency_code", "quantity_ordered", "quantity_dispensed",
                        "days_supply", "unit_cost", "total_cost", "is_discontinued",
                        "updated_at"],
                       med_rows)
        self.add_table("CLAIMS/claim_header",
                       ["claim_number", "patient_id", "provider_doctor_id", "department_id",
                        "hospital_id", "payer_id", "encounter_id", "primary_diagnosis_code",
                        "service_date", "submission_date", "adjudication_date",
                        "payment_date", "status_code", "billed_amount", "allowed_amount",
                        "approved_amount", "paid_amount", "denied_amount",
                        "denial_reason_code", "patient_responsibility",
                        "original_claim_number", "resubmission_count", "last_modified_ts"],
                       claim_hdr_rows)
        self.add_table("CLAIMS/claim_line",
                       ["claim_number", "line_number", "service_code", "service_category",
                        "quantity", "line_amount"], claim_line_rows)
        self.add_table("FIN/invoice",
                       ["invoice_id", "patient_id", "encounter_id", "hospital_id",
                        "payer_id", "invoice_date", "service_date", "total_amount",
                        "update_dt"], inv_rows)
        self.add_table("FIN/invoice_line",
                       ["invoice_id", "invoice_line_number", "patient_id", "department_id",
                        "hospital_id", "payer_id", "encounter_id", "service_code",
                        "service_category", "quantity", "charge_amount", "discount_amount",
                        "tax_amount", "net_amount", "payment_amount", "outstanding_amount"],
                       inv_line_rows)
        self.add_table("SURVEY/survey_response",
                       ["survey_response_id", "patient_id", "doctor_id", "department_id",
                        "hospital_id", "encounter_id", "response_date", "service_date",
                        "encounter_type", "overall_score", "wait_time_score",
                        "staff_courtesy_score", "cleanliness_score", "communication_score",
                        "pain_management_score", "would_recommend_score", "nps_category",
                        "has_free_text_comment"],
                       survey_rows)

    # ================================================================
    # OUTPUT
    # ================================================================

    def write(self, ingest_date: str):
        os.makedirs(self.out_dir, exist_ok=True)
        manifest = []
        for name, (header, rows) in sorted(self.tables.items()):
            source, entity = name.split("/")
            folder = os.path.join(self.out_dir, source, entity,
                                  f"ingest_date={ingest_date}")
            os.makedirs(folder, exist_ok=True)
            path = os.path.join(folder, f"{entity}_{ingest_date}.csv")
            with open(path, "w", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                w.writerow(header)
                w.writerows(rows)
            size_mb = os.path.getsize(path) / 1_048_576
            manifest.append((source, entity, len(rows), round(size_mb, 2), path))
        return manifest


# =====================================================================
# ENTRY POINT
# =====================================================================

def main():
    ap = argparse.ArgumentParser(description="Generate synthetic OHN source data")
    ap.add_argument("--patients", type=int, default=25000)
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2026-07-31")
    ap.add_argument("--out", default="./landing")
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--ingest-date", default="2026-08-01")
    ap.add_argument("--bed-scale", type=float, default=None,
                    help="Override automatic bed sizing (1.0 = real hospital size)")
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    print(f"Generating {args.patients:,} patients over {args.start} to {args.end}")
    gen = OHNGenerator(args.patients, start, end, args.out, args.seed, args.bed_scale)
    print(f"  bed inventory scaled to {gen.bed_scale:.3f} of real hospital size")

    print("  facilities and staff...")
    gen.gen_facilities()
    gen.gen_doctors()
    print("  reference data...")
    gen.gen_reference()
    print("  patient population and source projections...")
    gen.gen_people()
    gen.write_patient_tables()
    print("  clinical and financial activity...")
    gen.gen_activity()
    print("  writing files...")
    manifest = gen.write(args.ingest_date)

    print(f"\n{'Source':<10} {'Entity':<28} {'Rows':>10} {'MB':>8}")
    print("-" * 60)
    total_rows = total_mb = 0
    for source, entity, rows, mb, _ in manifest:
        print(f"{source:<10} {entity:<28} {rows:>10,} {mb:>8.2f}")
        total_rows += rows
        total_mb += mb
    print("-" * 60)
    print(f"{'TOTAL':<39} {total_rows:>10,} {total_mb:>8.2f}")
    print(f"\nWritten to {os.path.abspath(args.out)}")


if __name__ == "__main__":
    main()
