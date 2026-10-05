# مشروع خط أنابيب البيانات الضخمة الهجين (Hybrid ELT Big Data Pipeline)

```{=html}
<p align="center">
```
`<strong>`{=html}منصة متكاملة لمعالجة البيانات الضخمة باستخدام PySpark
وMongoDB وFastAPI`</strong>`{=html}
```{=html}
</p>
```
```{=html}
<p align="center">
```
`<em>`{=html}معالجة واسعة النطاق للبيانات، جودة البيانات، العزل
Quarantine، منع التكرار، تحسين الاستعلامات، التحليلات، Materialized
Views، والـ API`</em>`{=html}
```{=html}
</p>
```

------------------------------------------------------------------------

## 1. نبذة عن المشروع

هذا المشروع عبارة عن **منصة متكاملة لمعالجة البيانات الضخمة** تم تصميمها
لتنفيذ دورة كاملة تبدأ من استقبال البيانات الخام، ثم تنظيفها والتحقق من
جودتها، ومعالجة البيانات باستخدام Python أو PySpark حسب حجم الملف، ثم
تخزينها في MongoDB، وإجراء التحليلات عليها، وإنشاء Materialized Views،
وتشغيل المهام المجدولة، وأخيرًا إتاحة الوظائف من خلال REST API باستخدام
FastAPI.

المشروع لا يمثل مجرد سكربت لتنظيف البيانات، وإنما يطبق مجموعة من مفاهيم
**Big Data Engineering** و **Data Engineering** بصورة مترابطة.

### الهدف الرئيسي

> بناء خط أنابيب بيانات موثوق وقابل للتوسع يستطيع معالجة بيانات الطلبات
> كبيرة الحجم، اكتشاف وتصحيح مشاكل جودة البيانات، عزل السجلات غير
> القابلة للإصلاح، منع التكرار عند إعادة تشغيل المعالجة، تحسين أداء
> استعلامات MongoDB، إنتاج تقارير تحليلية، وتوفير واجهة API للوصول إلى
> وظائف النظام.

------------------------------------------------------------------------

# 2. لماذا يعتبر المشروع نظام Big Data متكاملًا؟

تم تصميم المشروع لمعالجة مجموعة من المشاكل الواقعية التي تظهر في أنظمة
البيانات:

-   **قابلية التوسع Scalability:** استخدام PySpark لمعالجة البيانات
    كبيرة الحجم.
-   **المعالجة الهجينة Hybrid Processing:** اختيار Python للملفات
    الصغيرة وPySpark للملفات الكبيرة.
-   **جودة البيانات Data Quality:** تطبيق 9 قواعد واضحة لتنظيف والتحقق
    من البيانات.
-   **الحوكمة Data Governance:** عدم حذف البيانات غير الصالحة مباشرة، بل
    عزلها في Quarantine.
-   **منع التكرار Idempotency:** استخدام `upsert` وUnique Index لمنع
    إنشاء سجلات مكررة.
-   **قابلية التدقيق Auditability:** الاحتفاظ بمعلومات تساعد على معرفة
    سبب تصحيح أو عزل السجل.
-   **تحسين الأداء Performance Optimization:** إنشاء MongoDB indexes
    واستخدام `explain("executionStats")`.
-   **التحليل Analytics:** تنفيذ 5 تقارير Aggregation.
-   **المعالجة المسبقة للتحليلات:** إنشاء Materialized Views.
-   **التحديث التدريجي Incremental Refresh:** استخدام Watermark لتحديث
    البيانات الجديدة فقط.
-   **الأتمتة Automation:** تشغيل Scheduled Jobs.
-   **المراقبة Monitoring:** تسجيل نتائج المهام وفحص صحة النظام.
-   **توفير API:** استخدام FastAPI لتوفير الخدمات من خلال HTTP.
-   **الاختبار Testing:** وجود اختبارات باستخدام Pytest.

------------------------------------------------------------------------

# 3. التقنيات المستخدمة

  الطبقة                    التقنية                       الاستخدام
  ------------------------- ----------------------------- -------------------------------------------
  لغة البرمجة               Python                        بناء منطق النظام والـ Pipeline
  المعالجة الموزعة          PySpark                       معالجة البيانات كبيرة الحجم
  قاعدة البيانات            MongoDB                       تخزين البيانات الخام والمعالجة والتحليلية
  الاتصال بقاعدة البيانات   PyMongo                       تنفيذ عمليات MongoDB
  API                       FastAPI                       توفير REST API
  خادم API                  Uvicorn                       تشغيل FastAPI
  الجدولة                   APScheduler / Job Framework   تشغيل المهام المجدولة
  الاختبارات                Pytest                        اختبار مكونات النظام
  معالجة البيانات           Pandas                        عمليات مساعدة لمعالجة البيانات
  الإعدادات                 `.env` / Settings             إدارة إعدادات البيئة

------------------------------------------------------------------------

# 4. المعمارية العامة للنظام

``` text
                         +----------------------+
                         |      ملف CSV          |
                         |  بيانات طلبات متنوعة  |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |     File Router      |
                         | اختيار محرك المعالجة |
                         | حسب حجم الملف        |
                         +----------+-----------+
                                    |
                   +----------------+----------------+
                   |                                 |
             ملف صغير/متوسط                       ملف كبير
                   |                                 |
                   v                                 v
          +----------------+                +----------------+
          | Python Batch   |                |    PySpark     |
          | Loader         |                | Distributed    |
          +-------+--------+                +-------+--------+
                  |                                 |
                  +----------------+----------------+
                                   |
                                   v
                         +----------------------+
                         |      orders_raw      |
                         |    البيانات الخام     |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |    9 Quality Rules   |
                         | تنظيف + تحقق + تصحيح |
                         +----------+-----------+
                                    |
                    +---------------+---------------+
                    |                               |
             بيانات صالحة/قابلة للتصحيح       بيانات غير قابلة للإصلاح
                    |                               |
                    v                               v
          +---------------------+        +----------------------+
          | orders_validated    |        | orders_quarantine    |
          | Idempotent Upsert   |        | سبب العزل + Audit    |
          +----------+----------+        +----------------------+
                     |
          +----------+-----------+------------------+
          |                      |                  |
          v                      v                  v
   +-------------+       +---------------+   +---------------+
   | MongoDB     |       | Aggregations  |   | Indexes +     |
   | Indexes     |       | 5 Reports     |   | Explain       |
   +-------------+       +-------+-------+   +---------------+
                                  |
                                  v
                         +----------------------+
                         | Materialized Views   |
                         | Incremental Refresh  |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Scheduled Jobs       |
                         | Refresh + Audit      |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |      FastAPI         |
                         |     REST API         |
                         +----------------------+
```

------------------------------------------------------------------------

# 5. مراحل معالجة البيانات

يمر النظام بعدة مراحل مترابطة.

## المرحلة الأولى: اختيار محرك المعالجة

يقوم `file_router.py` بتحديد طريقة المعالجة بناءً على حجم ملف الإدخال.

``` text
ملف صغير
   ↓
Python Batch Loader

ملف كبير
   ↓
PySpark
```

وبذلك لا يتم استخدام Spark دون الحاجة إليه، وفي الوقت نفسه يستطيع النظام
الانتقال إلى المعالجة الموزعة عند التعامل مع البيانات الكبيرة.

------------------------------------------------------------------------

# 6. طبقة البيانات الخام Raw Layer

يتم تخزين البيانات التي تم استقبالها في:

``` text
orders_raw
```

والهدف من هذه الطبقة هو الاحتفاظ بالبيانات الخام قبل عمليات التنظيف
والتحقق.

وجود Raw Layer مهم لأنه:

-   يحافظ على البيانات الأصلية.
-   يسمح بإعادة معالجة البيانات.
-   يزيد من قابلية التدقيق.
-   يفصل بين البيانات الأصلية والبيانات المعالجة.

------------------------------------------------------------------------

# 7. تنظيف وجودة البيانات Data Quality

يطبق المشروع **9 قواعد لجودة البيانات**:

### 1. توحيد الأرقام العربية

تحويل الأرقام المكتوبة بصيغ عربية إلى صيغة موحدة يمكن للنظام التعامل
معها.

### 2. توحيد العملات

تنظيف قيم الأسعار والتعامل مع رموز العملات المختلفة.

### 3. معالجة فواصل الآلاف

توحيد القيم الرقمية التي تحتوي على فواصل آلاف.

### 4. تحويل الأسعار المكتوبة بالكلمات

التعامل مع بعض قيم الأسعار التي قد تكون مكتوبة بصيغة نصية.

### 5. توحيد أرقام الهاتف

تنظيف وتوحيد أرقام الهواتف.

### 6. توحيد البريد الإلكتروني

تنظيف قيم البريد الإلكتروني وتوحيد طريقة تخزينها.

### 7. توحيد التواريخ

تحويل صيغ التواريخ المختلفة إلى صيغة موحدة.

### 8. معالجة المسافات والمرادفات

تنظيف النصوص وتوحيد بعض القيم المختلفة التي تشير إلى نفس المعنى.

### 9. إعادة حساب إجمالي الطلب

إعادة حساب إجمالي الطلب اعتمادًا على البيانات المتوفرة بدل الاعتماد على
قيمة قد تكون خاطئة في المصدر.

------------------------------------------------------------------------

# 8. Validated Data وQuarantine

بعد تنفيذ قواعد الجودة يتم تقسيم السجلات إلى مسارين:

``` text
                  البيانات المعالجة
                         |
             +-----------+-----------+
             |                       |
             v                       v
       Validated Data          Quarantine Data
             |                       |
             v                       v
  orders_validated          orders_quarantine
```

## `orders_validated`

تحتوي على البيانات التي أصبحت صالحة للاستخدام التحليلي.

## `orders_quarantine`

تحتوي على السجلات التي لم يكن من الممكن إصلاحها أو التحقق منها بصورة
موثوقة.

### لماذا Quarantine؟

بدل حذف السجلات غير الصالحة، يقوم النظام بعزلها.

وهذا يوفر:

-   عدم فقدان البيانات.
-   إمكانية مراجعة الأخطاء.
-   معرفة سبب رفض السجل.
-   إمكانية إصلاح البيانات مستقبلًا.
-   حماية التقارير والتحليلات من البيانات غير الموثوقة.

------------------------------------------------------------------------

# 9. Idempotency ومنع التكرار

من أهم خصائص المشروع **Idempotent Processing**.

أي أن تشغيل نفس البيانات أكثر من مرة لا يؤدي إلى إنشاء سجلات مكررة.

يتم تحقيق ذلك باستخدام:

-   `upsert=True`
-   `UpdateOne`
-   Bulk Operations
-   Unique Index على `order_id`
-   التحقق من نتائج إعادة المعالجة

بصورة مبسطة:

``` text
التشغيل الأول:

order_id = 1001
       ↓
    INSERT


التشغيل الثاني:

order_id = 1001
       ↓
    UPDATE


النتيجة:

لا يوجد Duplicate Business Record
```

وهذا مهم جدًا في أنظمة Big Data لأن Jobs قد يتم إعادة تشغيلها بعد فشل أو
انقطاع.

------------------------------------------------------------------------

# 10. اختبار المليون سجل

يتضمن المشروع Pipeline مخصصًا لمعالجة:

**1,000,000 سجل**

والنتائج المسجلة فعليًا هي:

  المؤشر                                     النتيجة
  ------------------------- ------------------------
  عدد السجلات الخام                    **1,000,000**
  عدد السجلات المعالجة                 **1,000,000**
  السجلات الصالحة/المصححة                **918,742**
  سجلات Quarantine                        **81,258**
  عدد قواعد جودة البيانات                      **9**
  زمن التنفيذ                      **215.946 ثانية**
  معدل المعالجة               **4,630.78 سجل/ثانية**
  عدد Worker Cores                             **8**
  الذاكرة                               **30.9 GiB**
  التحقق من Count                         **PASSED**
  Validated Upsert                        **PASSED**
  Quarantine Upsert                       **PASSED**
  Idempotency                             **PASSED**

### معدل المعالجة

بلغ معدل المعالجة:

``` text
4,630.78 records / second
```

وهذا يعطي Benchmark قابلًا للقياس يمكن استخدامه لاحقًا لمقارنة أي تحسينات
في أداء النظام.

------------------------------------------------------------------------

# 11. MongoDB Data Model

يستخدم المشروع عدة Collections:

  Collection                  الوظيفة
  --------------------------- ------------------------------
  `orders_raw`                البيانات الخام
  `orders_validated`          البيانات النظيفة والصالحة
  `orders_quarantine`         البيانات غير القابلة للإصلاح
  `job_logs`                  سجل تنفيذ المهام
  `mv_daily_sales_summary`    ملخص المبيعات اليومية
  `mv_top_products_summary`   ملخص أفضل المنتجات
  `mv_watermarks`             تخزين نقاط التحديث التدريجي

------------------------------------------------------------------------

# 12. MongoDB Indexing

يطبق المشروع مجموعة من الفهارس لتحسين أداء الاستعلامات.

## Compound Index

``` text
(customer_id ASC, order_date DESC)
```

يستخدم عند البحث عن طلبات العميل وترتيبها حسب التاريخ.

## Status Index

``` text
status ASC
```

يساعد على تسريع البحث حسب حالة الطلب.

## City Index

``` text
city ASC
```

يساعد على تسريع الاستعلامات التي تعتمد على المدينة.

------------------------------------------------------------------------

# 13. تحليل أداء الاستعلامات

لا يكتفي المشروع بإنشاء Indexes فقط، بل يقيس تأثيرها.

يتم استخدام:

``` python
explain("executionStats")
```

ثم مقارنة:

``` text
قبل Index
     ↓
Execution Stats
     ↓
إنشاء Index
     ↓
Execution Stats
     ↓
مقارنة الأداء
```

وتتم مقارنة عناصر مثل:

-   زمن التنفيذ.
-   عدد الوثائق التي تم فحصها.
-   عدد النتائج.
-   Query Plan.
-   Winning Plan.
-   هل تم استخدام Index أم لا.

وهذا يجعل تحسين قاعدة البيانات **قابلًا للقياس والإثبات** بدل أن يكون
مجرد إعداد نظري.

------------------------------------------------------------------------

# 14. الاستعلامات التحليلية

يحتوي المشروع على 5 استعلامات عملية:

1.  سجل طلبات العميل.
2.  الطلبات حسب المدينة وحالة التوصيل.
3.  الطلبات ضمن فترة زمنية.
4.  البحث باستخدام البريد الإلكتروني أو رقم الهاتف.
5.  البحث عن الطلبات مرتفعة القيمة.

ومن خلال API يمكن الوصول إليها مثلًا عبر:

``` text
GET /queries/customer_orders
GET /queries/city_status
GET /queries/date_range
GET /queries/email_or_phone
GET /queries/high_value_orders
GET /queries/explain_analysis
```

------------------------------------------------------------------------

# 15. Aggregation Reports

ينفذ المشروع خمسة تقارير تحليلية باستخدام MongoDB Aggregation Pipeline:

  التقرير              الوظيفة
  -------------------- -----------------------------------
  `sales_by_city`      تحليل المبيعات حسب المدينة
  `top_products`       معرفة المنتجات الأكثر مبيعًا
  `top_customers`      معرفة العملاء الأعلى إنفاقًا
  `sales_by_period`    تحليل المبيعات حسب الفترة الزمنية
  `orders_by_status`   توزيع الطلبات حسب الحالة

ميزة هذا الأسلوب أن المعالجة التحليلية تتم داخل MongoDB بدل نقل جميع
البيانات إلى Python.

------------------------------------------------------------------------

# 16. Materialized Views

يحتوي النظام على Materialized Views لتخزين نتائج تحليلية جاهزة.

## `mv_daily_sales_summary`

يحتوي على ملخص المبيعات اليومية مثل:

-   عدد الطلبات.
-   إجمالي الإيرادات.
-   وقت آخر تحديث.

## `mv_top_products_summary`

يحتوي على:

-   اسم المنتج.
-   الكمية المباعة.
-   إجمالي الإيرادات.
-   آخر وقت تحديث.

------------------------------------------------------------------------

# 17. Incremental Refresh

من المميزات المهمة في المشروع أن Materialized Views لا تحتاج دائمًا إلى
إعادة حساب جميع البيانات.

يتم استخدام:

``` text
last_at_ingested
```

كـ Watermark.

الفكرة:

``` text
البيانات القديمة
      ↓
تمت معالجتها سابقًا
      ↓
لا تتم إعادة معالجتها


البيانات الجديدة
      ↓
أحدث من Watermark
      ↓
تتم إضافتها إلى الـ View
```

وهذا يقلل من تكلفة إعادة الحساب ويجعل النظام أكثر كفاءة.

------------------------------------------------------------------------

# 18. Scheduled Jobs

يحتوي المشروع على مهام مجدولة.

## `refresh_materialized_views_job`

تقوم بتحديث Materialized Views.

## `periodic_system_audit_job`

تقوم بمراجعة حالة النظام، مثل:

-   عدد السجلات الصالحة.
-   عدد سجلات Quarantine.
-   إجمالي البيانات المعالجة.
-   الحالة العامة للنظام.

يتم تسجيل نتائج المهام في:

``` text
job_logs
```

وتشمل المعلومات:

-   اسم المهمة.
-   وقت البداية.
-   وقت النهاية.
-   مدة التنفيذ.
-   حالة المهمة.
-   تفاصيل التنفيذ.
-   معلومات الخطأ عند حدوثه.

------------------------------------------------------------------------

# 19. FastAPI

تم توفير واجهة REST API باستخدام FastAPI.

## أهم Endpoints

  Method   Endpoint                       الوظيفة
  -------- ------------------------------ --------------------------------
  `GET`    `/health`                      فحص صحة النظام وقاعدة البيانات
  `POST`   `/ingest`                      تشغيل عملية الإدخال
  `POST`   `/indexes`                     إنشاء Indexes
  `GET`    `/queries`                     عرض الاستعلامات
  `GET`    `/queries/{name}`              تنفيذ استعلام
  `GET`    `/aggregations`                عرض التقارير
  `GET`    `/aggregations/{name}`         تنفيذ تقرير
  `POST`   `/refresh-mv`                  تحديث Materialized Views
  `GET`    `/materialized-views/{name}`   قراءة Materialized View
  `GET`    `/jobs`                        عرض المهام وسجلاتها
  `POST`   `/jobs/{name}/run`             تشغيل مهمة يدويًا

------------------------------------------------------------------------

# 20. Swagger / OpenAPI

بعد تشغيل الـAPI يمكن فتح:

``` text
http://localhost:8000/docs
```

وسيظهر توثيق تفاعلي يسمح بتجربة الـEndpoints مباشرة من المتصفح.

------------------------------------------------------------------------

# 21. هيكل المشروع

``` text
.
├── config/
│   └── settings.py
│
├── data/
│   ├── small_sample.csv
│   ├── million_sample.csv
│   └── orders_huge_mixed_quality.csv
│
├── reports/
│   ├── results.md
│   ├── results.json
│   └── evidence/
│
├── tools/
│   ├── inspect_doc.py
│   ├── test_conversion.py
│   └── test_products.py
│
├── pipeline/
│   ├── aggregation_reports.py
│   ├── api_service.py
│   ├── batch_ingestion.py
│   ├── generate_million_dataset.py
│   ├── generate_sample_dataset.py
│   ├── million_row_pipeline.py
│   ├── elt_pipeline.py
│   ├── file_router.py
│   ├── run_pipeline.py
│   ├── materialized_views.py
│   ├── pipeline_metrics.py
│   ├── mongodb_setup.py
│   ├── queries_and_indexes.py
│   ├── data_quality.py
│   ├── job_scheduler.py
│   └── spark_ingestion.py
│
├── tests/
│   ├── test_aggregation_reports.py
│   ├── test_api_service.py
│   ├── test_data_classification.py
│   ├── test_data_quality.py
│   ├── test_materialized_views.py
│   ├── test_queries_and_indexes.py
│   └── test_job_scheduler.py
│
├── .env.example
├── requirements.txt
└── README.md
```

------------------------------------------------------------------------

# 22. تثبيت المشروع

## 22.1 استنساخ المشروع

``` bash
git clone <YOUR-REPOSITORY-URL>
cd <PROJECT-DIRECTORY>
```

------------------------------------------------------------------------

## 22.2 إنشاء Virtual Environment

### Windows PowerShell

``` powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### Linux / macOS

``` bash
python3 -m venv venv
source venv/bin/activate
```

------------------------------------------------------------------------

## 22.3 تثبيت المكتبات

``` bash
pip install -r requirements.txt
```

------------------------------------------------------------------------

# 23. إعداد MongoDB

الإعداد الافتراضي للمشروع يستخدم:

``` text
mongodb://localhost:27017
```

وقاعدة البيانات:

``` text
midterm_data_pipeline2
```

إذا كان MongoDB يعمل بإعداد مختلف، يجب تعديل إعدادات الاتصال وفق البيئة
المستخدمة.

------------------------------------------------------------------------

# 24. تهيئة MongoDB

يمكن تشغيل:

``` bash
python -m pipeline.mongodb_setup
```

لتجهيز قاعدة البيانات والـIndexes المطلوبة.

------------------------------------------------------------------------

# 25. تشغيل المشروع

لتشغيل الـworkflow الرئيسي:

``` bash
python -m pipeline.run_pipeline
```

ويقوم النظام بتنفيذ المراحل الأساسية:

``` text
1. File Routing + Raw Ingestion
        ↓
2. ELT Cleaning + Quality Validation
        ↓
3. Idempotency Verification
        ↓
4. Index Creation + Explain Analysis
        ↓
5. Aggregation Reports
        ↓
6. Materialized View Refresh
        ↓
7. Scheduled Jobs + Audit Logs
        ↓
8. FastAPI
```

------------------------------------------------------------------------

# 26. تشغيل Pipeline المليون سجل

يمكن إنشاء بيانات الاختبار عند الحاجة:

``` bash
python -m pipeline.generate_million_dataset
```

ثم تشغيل Pipeline الخاص بالمليون سجل:

``` bash
python -m pipeline.million_row_pipeline
```

يستخدم هذا الـPipeline PySpark لمعالجة البيانات ثم تنفيذ Bulk Upsert إلى
MongoDB.

------------------------------------------------------------------------

# 27. تشغيل FastAPI بشكل مستقل

``` bash
uvicorn pipeline.api_service:app --host 0.0.0.0 --port 8000
```

بعد ذلك:

``` text
http://localhost:8000/docs
```

ولفحص حالة النظام:

``` text
GET /health
```

------------------------------------------------------------------------

# 28. الاختبارات

يحتوي المشروع على اختبارات باستخدام Pytest تغطي:

-   Aggregations.
-   API.
-   Data Classification.
-   Cleaning Rules.
-   Materialized Views.
-   Queries and Indexes.
-   Scheduled Jobs.

لتشغيل الاختبارات:

``` bash
pytest -v
```

------------------------------------------------------------------------

# 29. نتائج المشروع والأدلة

يحتوي المشروع على مجلد:

``` text
reports/
```

ويشمل:

``` text
reports/results.md
reports/results.json
reports/evidence/
```

## `results.md`

يحتوي على النتائج المقروءة للبشر.

## `results.json`

يحتوي على النتائج بصيغة قابلة للمعالجة البرمجية.

## `evidence/`

يحتوي على الأدلة المرئية المتعلقة بتنفيذ المشروع، مثل:

-   MongoDB Indexes.
-   Validated Collection.
-   Quarantine Collection.
-   Idempotency Verification.
-   PySpark Processing.

------------------------------------------------------------------------

# 30. القرارات التصميمية

## لماذا نحتفظ بـ Raw Collection؟

للحفاظ على البيانات الأصلية، وتوفير إمكانية إعادة المعالجة والتدقيق.

------------------------------------------------------------------------

## لماذا نستخدم Quarantine بدل حذف البيانات؟

لأن حذف البيانات غير الصالحة يؤدي إلى فقدان المعلومات.

أما Quarantine فيسمح:

``` text
اكتشاف الخطأ
    ↓
عزل السجل
    ↓
معرفة السبب
    ↓
مراجعة السجل
    ↓
إمكانية إصلاحه مستقبلًا
```

------------------------------------------------------------------------

## لماذا نستخدم Upsert؟

لأن الـPipeline قد يعمل أكثر من مرة بسبب:

-   Failure.
-   Retry.
-   إعادة تشغيل Job.
-   إعادة إرسال نفس الملف.

لذلك يجب ألا يؤدي إعادة التشغيل إلى إنشاء Duplicate Records.

------------------------------------------------------------------------

## لماذا نستخدم Unique Index؟

حتى يكون منع التكرار مدعومًا على مستوى قاعدة البيانات وليس فقط في كود
التطبيق.

------------------------------------------------------------------------

## لماذا نستخدم Materialized Views؟

لأن بعض التحليلات قد تكون مكلفة إذا تم حسابها في كل طلب.

لذلك يتم حساب النتائج وتخزينها مسبقًا لتسريع الوصول إليها.

------------------------------------------------------------------------

## لماذا نستخدم Explain؟

لأن إنشاء Index لا يعني تلقائيًا أن الأداء أصبح أفضل.

لذلك يتم استخدام:

``` text
explain("executionStats")
```

لقياس تأثير الـIndex بشكل فعلي.

------------------------------------------------------------------------

## لماذا نستخدم FastAPI؟

لفصل منطق معالجة البيانات عن طريقة الوصول إليه، وإتاحة:

-   Ingestion.
-   Queries.
-   Aggregations.
-   Materialized Views.
-   Jobs.
-   Health Checks.

من خلال REST API موحدة.

------------------------------------------------------------------------

# 31. الاعتمادية Reliability

يجمع المشروع عدة آليات لزيادة موثوقية النظام:

``` text
Raw Data Preservation
        +
Data Validation
        +
Quarantine
        +
Audit Information
        +
Idempotent Upsert
        +
Unique Business-Key Constraint
        +
Job Logging
        +
Health Check
        +
Automated Tests
```

وهذا يقلل من احتمالية:

-   تكرار البيانات.
-   فقدان البيانات بصمت.
-   دخول البيانات غير الصالحة إلى التحليلات.
-   عدم معرفة فشل الـJobs.
-   حدوث مشاكل عند إعادة تشغيل Pipeline.

------------------------------------------------------------------------

# 32. تغطية متطلبات Big Data

  المفهوم                  التطبيق داخل المشروع
  ------------------------ ----------------------------
  Big Data Processing      PySpark
  Data Ingestion           Python Batch + Spark
  Hybrid Processing        File Router
  ELT                      Raw → Validate → Store
  Data Quality             9 Quality Rules
  Data Cleaning            Normalization + Correction
  Data Validation          Validated / Quarantine
  Data Governance          Quarantine + Audit
  Idempotency              Upsert + Unique Index
  Database Optimization    MongoDB Indexes
  Query Performance        Explain Execution Stats
  Analytics                5 Aggregation Reports
  Materialized Analytics   Materialized Views
  Incremental Processing   Watermark
  Automation               Scheduled Jobs
  Monitoring               Job Logs + Health Endpoint
  API Engineering          FastAPI
  Testing                  Pytest

------------------------------------------------------------------------

# 33. متطلبات البيئة

قبل تشغيل المشروع يجب التأكد من:

1.  تشغيل MongoDB.
2.  تثبيت Python.
3.  تثبيت جميع المكتبات الموجودة في `requirements.txt`.
4.  وجود Java Runtime متوافق مع إصدار PySpark المستخدم.
5.  وجود ملفات CSV داخل مجلد `data/`.
6.  أن إعدادات Spark مناسبة للبيئة المحلية أو الـCluster.
7.  أن إعدادات MongoDB صحيحة.

> ملاحظة: إعدادات Spark وMongoDB تختلف حسب بيئة التشغيل، لذلك يجب
> اعتبارها Environment-Specific Configuration.

------------------------------------------------------------------------

# 34. التطويرات المستقبلية

يمكن تطوير المشروع مستقبلًا بإضافة:

-   Docker وDocker Compose.
-   CI/CD باستخدام GitHub Actions.
-   Kafka للـStreaming.
-   Apache Airflow لإدارة الـWorkflows.
-   Prometheus وGrafana للمراقبة.
-   Cloud Object Storage مثل S3.
-   Schema Evolution.
-   Schema Registry.
-   Authentication وAuthorization للـAPI.
-   Load Testing.
-   Integration Testing أكثر شمولًا.
-   Data Quality Dashboard.
-   Centralized Logging.

------------------------------------------------------------------------

# 35. الخلاصة

هذا المشروع يمثل **نظام Big Data ELT متكامل** وليس مجرد برنامج لتنظيف
البيانات.

يمثل النظام دورة معالجة كاملة:

``` text
INGEST
   ↓
ROUTE
   ↓
PROCESS
   ↓
CLEAN
   ↓
VALIDATE
   ↓
QUARANTINE
   ↓
UPSERT
   ↓
OPTIMIZE
   ↓
ANALYZE
   ↓
MATERIALIZE
   ↓
SCHEDULE
   ↓
SERVE THROUGH API
   ↓
TEST & MONITOR
```

وقد تم اختبار النظام على:

**1,000,000 سجل**

مع النتائج التالية:

-   **1,000,000** سجل تمت معالجتها.
-   **918,742** سجل تم قبولها/تصحيحها.
-   **81,258** سجل تم عزلها في Quarantine.
-   **4,630.78 سجل/ثانية** معدل معالجة.
-   **215.946 ثانية** زمن تنفيذ.
-   نجاح اختبار **Idempotency**.
-   نجاح عمليات **Validated Upsert**.
-   نجاح عمليات **Quarantine Upsert**.

وبالإضافة إلى معالجة البيانات، يتضمن المشروع:

**Data Quality + Quarantine + Idempotency + MongoDB Indexing + Explain
Analysis + Aggregations + Materialized Views + Incremental Refresh +
Scheduled Jobs + FastAPI + Automated Tests**

مما يجعله تطبيقًا عمليًا لمفاهيم **Big Data Engineering** و**Data
Engineering** في مشروع واحد متكامل.

------------------------------------------------------------------------

# 36. مخرجات المشروع

يتضمن المستودع:

-   Source Code كامل.
-   Python Batch Pipeline.
-   PySpark Pipeline.
-   Data Quality Rules.
-   MongoDB Integration.
-   Quarantine Mechanism.
-   Idempotent Upsert.
-   MongoDB Indexes.
-   Explain Performance Analysis.
-   Aggregation Reports.
-   Materialized Views.
-   Incremental Refresh.
-   Scheduled Jobs.
-   Job Logs.
-   FastAPI Service.
-   Automated Tests.
-   Million-Row Benchmark.
-   Results Reports.
-   Execution Screenshots.
-   Configuration Template.

------------------------------------------------------------------------

## المشروع في سطر واحد

> **منصة Big Data ELT هجينة وقابلة للتوسع لمعالجة بيانات الطلبات
> باستخدام Python وPySpark، مع جودة بيانات وحوكمة ومنع للتكرار وتحسين
> لقواعد البيانات وتحليلات وMaterialized Views وجدولة وREST API
> واختبارات آلية.**

---

# 37. معيار تسمية الملفات والمجلدات

تم تنظيم المشروع وفق مبدأ **واضح، وصفي، ومتوافق مع Python**:

- `pipeline/` بدل `pipeline/` لتوضيح أن المجلد يحتوي مكونات الـData Pipeline.
- `tools/` بدل `scratch/` للأدوات المساعدة.
- `reports/evidence/` بدل `reports/evidence/` لتوضيح أن الصور جزء من أدلة التقييم.
- أسماء الملفات أصبحت وصفية مثل `aggregation_reports.py` و`data_quality.py` و`queries_and_indexes.py`.
- تم حذف ملفات `__pycache__` و`.pytest_cache` من نسخة التسليم لأنها ملفات مولدة وليست جزءًا من Source Code.

