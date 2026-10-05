from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
)


# ============================================================
# AUDIT
# ============================================================

CORRECTION_SCHEMA = ArrayType(
    StructType([
        StructField("field", StringType(), True),
        StructField("original_value", StringType(), True),
        StructField("corrected_value", StringType(), True),
        StructField("rule_code", StringType(), True),
    ])
)


def clean_text(col):
    return F.regexp_replace(
        F.trim(col),
        r"\s+",
        " "
    )


def arabic_to_western(col):
    return F.translate(
        col,
        "٠١٢٣٤٥٦٧٨٩",
        "0123456789"
    )


def normalize_numeric_text(col):
    value = arabic_to_western(col)

    value = F.regexp_replace(
        value,
        "٫",
        "."
    )

    value = F.regexp_replace(
        value,
        "٬",
        ""
    )

    value = F.regexp_replace(
        value,
        ",",
        ""
    )

    return F.trim(value)


def normalize_currency_text(col):

    value = clean_text(col)

    return (
        F.when(
            F.lower(value).isin(
                "yer",
                "y.r",
                "y.r.",
                "ر.ي"
            ),
            F.lit("YER")
        )
        .when(
            value.contains("ريال"),
            F.lit("YER")
        )
        .otherwise(value)
    )


def remove_currency(col):

    value = arabic_to_western(col)

    return F.trim(
        F.regexp_replace(
            value,
            r"(?i)\s*(ريال\s*يمني|ريال|ر\.ي|YER|Y\.R\.?)\s*",
            ""
        )
    )


def correction_struct(
    field,
    original,
    corrected,
    rule_code
):

    return F.struct(
        F.lit(field).alias("field"),
        original.cast("string").alias("original_value"),
        corrected.cast("string").alias("corrected_value"),
        F.lit(rule_code).alias("rule_code")
    )


def add_audit(
    df,
    original,
    corrected,
    field,
    rule_code
):

    change = F.when(
        original.isNotNull()
        & corrected.isNotNull()
        & (
            original.cast("string")
            != corrected.cast("string")
        ),
        correction_struct(
            field,
            original,
            corrected,
            rule_code
        )
    )

    return (
        df
        .withColumn(
            "_new_correction",
            change
        )
        .withColumn(
            "corrections",
            F.when(
                F.col("_new_correction").isNotNull(),
                F.concat(
                    F.col("corrections"),
                    F.array(
                        F.col("_new_correction")
                    )
                )
            ).otherwise(
                F.col("corrections")
            )
        )
        .drop("_new_correction")
    )


# ============================================================
# RULE 1 — ARABIC NUMBERS
# ============================================================

def rule_arabic_numbers(df):

    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ]:

        original = F.col(field)

        corrected = arabic_to_western(
            original
        )

        df = add_audit(
            df,
            original,
            corrected,
            field,
            "ARABIC_NUMBERS"
        )

        df = df.withColumn(
            field,
            corrected
        )

    return df


# ============================================================
# RULE 2 — CURRENCY
# ============================================================

def rule_currency(df):

    original = F.col("currency")

    corrected = normalize_currency_text(
        original
    )

    df = add_audit(
        df,
        original,
        corrected,
        "currency",
        "CURRENCY_NORMALIZATION"
    )

    df = df.withColumn(
        "currency",
        corrected
    )

    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ]:

        original = F.col(field)

        corrected = remove_currency(
            original
        )

        df = add_audit(
            df,
            original,
            corrected,
            field,
            "CURRENCY_REMOVAL"
        )

        df = df.withColumn(
            field,
            corrected
        )

    return df


# ============================================================
# RULE 3 — THOUSANDS SEPARATORS
# ============================================================

def rule_thousands(df):

    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ]:

        original = F.col(field)

        corrected = normalize_numeric_text(
            original
        )

        df = add_audit(
            df,
            original,
            corrected,
            field,
            "THOUSANDS_SEPARATOR"
        )

        df = df.withColumn(
            field,
            corrected
        )

    return df


# ============================================================
# RULE 4 — PRICE IN WORDS
# ============================================================

def rule_price_words(df):

    original = F.col(
        "delivery_cost"
    )

    value = F.trim(original)

    corrected = (
        F.when(value == "صفر", "0")
        .when(value == "واحد", "1")
        .when(value.isin("اثنان", "اثنين"), "2")
        .when(value == "ثلاثة", "3")
        .when(value == "أربعة", "4")
        .when(value == "خمسة", "5")
        .when(value == "ستة", "6")
        .when(value == "سبعة", "7")
        .when(value == "ثمانية", "8")
        .when(value == "تسعة", "9")
        .when(value == "عشرة", "10")
        .otherwise(original)
    )

    df = add_audit(
        df,
        original,
        corrected,
        "delivery_cost",
        "PRICE_IN_WORDS"
    )

    return df.withColumn(
        "delivery_cost",
        corrected
    )


# ============================================================
# RULE 5 — PHONE
# ============================================================

def rule_phone(df):

    original = F.col(
        "customer_phone"
    )

    corrected = arabic_to_western(
        original
    )

    corrected = F.regexp_replace(
        corrected,
        r"[\s\-\(\)\.]",
        ""
    )

    corrected = F.regexp_replace(
        corrected,
        r"^\+967",
        "967"
    )

    corrected = F.regexp_replace(
        corrected,
        r"^00967",
        "967"
    )

    corrected = F.when(
        corrected.rlike(r"^07\d{8}$"),
        F.concat(
            F.lit("967"),
            F.substring(
                corrected,
                2,
                20
            )
        )
    ).otherwise(corrected)

    corrected = F.when(
        corrected.rlike(r"^7\d{8}$"),
        F.concat(
            F.lit("967"),
            corrected
        )
    ).otherwise(corrected)

    df = add_audit(
        df,
        original,
        corrected,
        "customer_phone",
        "PHONE_NORMALIZATION"
    )

    return df.withColumn(
        "customer_phone",
        corrected
    )


# ============================================================
# RULE 6 — EMAIL
# ============================================================

def rule_email(df):

    original = F.col(
        "customer_email"
    )

    corrected = F.regexp_replace(
        F.trim(original),
        r"\s+",
        ""
    )

    corrected = F.regexp_replace(
        corrected,
        r"@{2,}",
        "@"
    )

    corrected = F.regexp_replace(
        corrected,
        r"\.{2,}",
        "."
    )

    df = add_audit(
        df,
        original,
        corrected,
        "customer_email",
        "EMAIL_NORMALIZATION"
    )

    return df.withColumn(
        "customer_email",
        corrected
    )


# ============================================================
# RULE 7 — DATE
# ============================================================

def rule_date(df):

    original = F.col(
        "order_date"
    )

    normalized = arabic_to_western(
        original
    )

    parsed = F.coalesce(

        F.try_to_timestamp(
            normalized,
            F.lit("yyyy-MM-dd'T'HH:mm:ss")
        ),

        F.try_to_timestamp(
            normalized,
            F.lit("yyyy-MM-dd HH:mm:ss")
        ),

        F.try_to_timestamp(
            normalized,
            F.lit("dd/MM/yyyy HH:mm:ss")
        ),

        F.try_to_timestamp(
            normalized,
            F.lit("dd-MM-yyyy HH:mm:ss")
        ),

        F.try_to_timestamp(
            normalized,
            F.lit("yyyy/MM/dd HH:mm:ss")
        ),

        F.try_to_timestamp(
            normalized,
            F.lit("dd/MM/yyyy")
        ),

        F.try_to_timestamp(
            normalized,
            F.lit("dd-MM-yyyy")
        ),

        F.try_to_timestamp(
            normalized,
            F.lit("yyyy-MM-dd")
        ),

        F.try_to_timestamp(
            normalized,
            F.lit("yyyy/MM/dd")
        )
    )

    corrected = F.when(
        parsed.isNotNull(),
        F.date_format(
            parsed,
            "yyyy-MM-dd"
        )
    ).otherwise(original)

    df = add_audit(
        df,
        original,
        corrected,
        "order_date",
        "DATE_NORMALIZATION"
    )

    return df.withColumn(
        "order_date",
        corrected
    )


# ============================================================
# RULE 8 — SPACES / SYNONYMS
# ============================================================

def rule_spaces_synonyms(df):

    fields = [
        "order_id",
        "status",
        "customer_id",
        "customer_name",
        "customer_phone",
        "customer_email",
        "city",
        "district",
        "delivery_type",
        "payment_method",
        "payment_status",
        "currency",
    ]

    for field in fields:

        original = F.col(field)

        corrected = clean_text(
            original
        )

        corrected = (
            F.when(
                F.lower(corrected) == "confirmed",
                "مؤكد"
            )
            .when(
                F.lower(corrected) == "pending",
                "قيد الانتظار"
            )
            .when(
                F.lower(corrected) == "shipped",
                "قيد الشحن"
            )
            .when(
                F.lower(corrected) == "returned",
                "مرتجع"
            )
            .otherwise(corrected)
        )

        df = add_audit(
            df,
            original,
            corrected,
            field,
            "SPACES_SYNONYMS"
        )

        df = df.withColumn(
            field,
            corrected
        )

    return df


# ============================================================
# RULE 9 — TOTAL ORDER
# ============================================================

def rule_total_order(df):

    item_schema = ArrayType(
        StructType([
            StructField("sku", StringType(), True),
            StructField("name", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("total", StringType(), True),
        ])
    )

    # --------------------------------------------------------
    # Parse JSON
    # --------------------------------------------------------

    df = df.withColumn(
        "_parsed_items",
        F.from_json(
            F.col("items_json"),
            item_schema
        )
    )

    df = df.withColumn(
        "_items_json_valid",
        F.col("_parsed_items").isNotNull()
    )

    df = df.withColumn(
        "_items_non_empty",
        F.size(
            F.col("_parsed_items")
        ) > 0
    )

    # --------------------------------------------------------
    # Normalize Arabic numeric values
    # --------------------------------------------------------

    df = df.withColumn(
        "_normalized_items",

        F.transform(
            F.col("_parsed_items"),

            lambda item: F.struct(

                item["sku"].alias("sku"),

                item["name"].alias("name"),

                normalize_numeric_text(
                    item["qty"]
                ).alias("qty"),

                normalize_numeric_text(
                    item["unit_price"]
                ).alias("unit_price"),

                normalize_numeric_text(
                    item["total"]
                ).alias("total"),
            )
        )
    )

    # --------------------------------------------------------
    # SQL TRY_CAST
    #
    # IMPORTANT:
    # Do NOT use F.try_cast().
    # Your PySpark version does not expose it.
    # --------------------------------------------------------

    df = df.withColumn(
        "_safe_items",

        F.expr(
            """
            transform(
                _normalized_items,
                item -> named_struct(
                    'sku', item.sku,
                    'name', item.name,

                    'qty',
                    try_cast(item.qty AS DOUBLE),

                    'unit_price',
                    try_cast(item.unit_price AS DOUBLE),

                    'total',
                    try_cast(item.total AS DOUBLE)
                )
            )
            """
        )
    )

    # --------------------------------------------------------
    # Validate items
    # --------------------------------------------------------

    df = df.withColumn(
        "_items_are_valid",

        F.expr(
            """
            aggregate(
                _safe_items,
                true,

                (ok, item) ->
                    ok
                    AND item.qty IS NOT NULL
                    AND item.unit_price IS NOT NULL
                    AND item.qty > 0
                    AND item.unit_price >= 0
            )
            """
        )
    )

    # --------------------------------------------------------
    # Normalize delivery cost
    # --------------------------------------------------------

    df = df.withColumn(
        "_delivery_normalized",

        normalize_numeric_text(
            F.col("delivery_cost")
        )
    )

    # --------------------------------------------------------
    # SQL TRY_CAST delivery cost
    # --------------------------------------------------------

    df = df.withColumn(
        "_delivery_numeric",

        F.expr(
            """
            try_cast(
                _delivery_normalized
                AS DOUBLE
            )
            """
        )
    )

    # --------------------------------------------------------
    # Calculate items total
    # --------------------------------------------------------

    df = df.withColumn(
        "_calculated_items",

        F.expr(
            """
            aggregate(
                _safe_items,

                CAST(0.0 AS DOUBLE),

                (acc, item) ->
                    acc
                    + (
                        item.qty
                        * item.unit_price
                    )
            )
            """
        )
    )

    # --------------------------------------------------------
    # Calculate complete order total
    # --------------------------------------------------------

    df = df.withColumn(
        "_calculated_total",

        F.when(
            F.col("_items_json_valid")
            & F.col("_items_non_empty")
            & F.col("_items_are_valid")
            & F.col("_delivery_numeric").isNotNull()
            & (
                F.col("_delivery_numeric") >= 0
            ),

            F.col("_calculated_items")
            + F.col("_delivery_numeric")
        )
    )

    # --------------------------------------------------------
    # Correct total_amount
    # --------------------------------------------------------

    original_total = F.col(
        "total_amount"
    )

    normalized_total = normalize_numeric_text(
        original_total
    )

    corrected_total = F.when(
        F.col("_calculated_total").isNotNull(),

        F.format_number(
            F.col("_calculated_total"),
            2
        )

    ).otherwise(
        normalized_total
    )

    df = add_audit(
        df,
        original_total,
        corrected_total,
        "total_amount",
        "TOTAL_ORDER_RECALCULATION"
    )

    df = df.withColumn(
        "total_amount",
        corrected_total
    )

    # --------------------------------------------------------
    # Error classification
    # --------------------------------------------------------

    df = df.withColumn(

        "_total_order_error",

        F.when(
            ~F.col("_items_json_valid"),
            "CORRUPTED_ITEMS_JSON"
        )

        .when(
            ~F.col("_items_non_empty"),
            "EMPTY_ITEMS"
        )

        .when(
            ~F.col("_items_are_valid"),
            "AMBIGUOUS_NEGATIVE_VALUE"
        )

        .when(
            F.col("_delivery_numeric").isNull(),
            "INVALID_DELIVERY_COST"
        )

        .when(
            F.col("_delivery_numeric") < 0,
            "AMBIGUOUS_NEGATIVE_VALUE"
        )
    )

    return df


# ============================================================
# VALIDATION
# ============================================================

def apply_validation(df):

    errors = F.array()

    # Missing order ID
    errors = F.when(

        F.col("order_id").isNull()
        | (
            F.trim(
                F.col("order_id")
            ) == ""
        ),

        F.array(
            F.lit("MISSING_ORDER_ID")
        )

    ).otherwise(errors)

    # Missing customer ID
    errors = F.when(

        F.col("customer_id").isNull()
        | (
            F.trim(
                F.col("customer_id")
            ) == ""
        ),

        F.array_union(
            errors,
            F.array(
                F.lit(
                    "MISSING_CUSTOMER_ID"
                )
            )
        )

    ).otherwise(errors)

    # Email
    errors = F.when(

        F.col("customer_email").isNotNull()
        & (
            F.trim(
                F.col("customer_email")
            ) != ""
        )
        & ~F.col(
            "customer_email"
        ).rlike(
            r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        ),

        F.array_union(
            errors,
            F.array(
                F.lit(
                    "INVALID_EMAIL"
                )
            )
        )

    ).otherwise(errors)

    # Phone
    errors = F.when(

        F.col("customer_phone").isNotNull()
        & (
            F.trim(
                F.col("customer_phone")
            ) != ""
        )
        & ~F.col(
            "customer_phone"
        ).rlike(
            r"^9677\d{8}$"
        ),

        F.array_union(
            errors,
            F.array(
                F.lit(
                    "INVALID_PHONE"
                )
            )
        )

    ).otherwise(errors)

    # Date
    errors = F.when(

        ~F.col(
            "order_date"
        ).rlike(
            r"^\d{4}-\d{2}-\d{2}$"
        ),

        F.array_union(
            errors,
            F.array(
                F.lit(
                    "INVALID_IMPOSSIBLE_DATE"
                )
            )
        )

    ).otherwise(errors)

    # Total order
    errors = F.when(

        F.col(
            "_total_order_error"
        ).isNotNull(),

        F.array_union(
            errors,
            F.array(
                F.col(
                    "_total_order_error"
                )
            )
        )

    ).otherwise(errors)

    # Unknown price
    errors = F.when(

        F.col("delivery_cost").isNotNull()
        & (
            F.trim(
                F.col("delivery_cost")
            ) != ""
        )
        & ~F.col(
            "delivery_cost"
        ).rlike(
            r"^-?\d+(\.\d+)?$"
        ),

        F.array_union(
            errors,
            F.array(
                F.lit(
                    "UNKNOWN_PRICE"
                )
            )
        )

    ).otherwise(errors)

    errors = F.array_distinct(
        F.filter(
            errors,
            lambda x: x.isNotNull()
        )
    )

    return df.withColumn(
        "quarantine_reasons",
        errors
    )


# ============================================================
# ALL 9 RULES
# ============================================================

def apply_all_quality_rules(df):

    # --------------------------------------------------------
    # Expand record_raw
    # --------------------------------------------------------

    df = df.select(
        "_id",
        "at_ingested",
        "engine_used",
        "file_source",
        "id_run",
        "number_row_source",
        "record_raw.*"
    )

    # --------------------------------------------------------
    # Empty audit trail
    # --------------------------------------------------------

    df = df.withColumn(
        "corrections",
        F.array().cast(
            CORRECTION_SCHEMA
        )
    )

    # --------------------------------------------------------
    # Nine rules
    # --------------------------------------------------------

    df = rule_arabic_numbers(df)

    df = rule_currency(df)

    df = rule_thousands(df)

    df = rule_price_words(df)

    df = rule_phone(df)

    df = rule_email(df)

    df = rule_date(df)

    df = rule_spaces_synonyms(df)

    df = rule_total_order(df)

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    df = apply_validation(df)

    # --------------------------------------------------------
    # Final classification
    # --------------------------------------------------------

    df = df.withColumn(

        "quality_status",

        F.when(
            F.size(
                F.col(
                    "quarantine_reasons"
                )
            ) > 0,

            F.lit(
                "quarantine"
            )
        )

        .when(
            F.size(
                F.col(
                    "corrections"
                )
            ) > 0,

            F.lit(
                "corrected"
            )
        )

        .otherwise(
            F.lit(
                "valid"
            )
        )
    )

    # --------------------------------------------------------
    # Remove temporary columns
    # --------------------------------------------------------

    df = df.drop(
        "_parsed_items",
        "_items_json_valid",
        "_items_non_empty",
        "_normalized_items",
        "_safe_items",
        "_items_are_valid",
        "_delivery_normalized",
        "_delivery_numeric",
        "_calculated_items",
        "_calculated_total",
        "_total_order_error",
    )

    return df