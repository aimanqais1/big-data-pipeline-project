# Comprehensive Viva Examination & Defense Guide (30 Questions)
**Course:** Big Data – Practical (Midterm Examination)  
**Project:** Hybrid Data Engineering Pipeline for E-Commerce Orders  
**Target:** Individual Student Oral Defense  
**Dataset Scale:** 100K Sample Test & 30,000,000 Rows (12.65 GB) Full-Scale

---

## Question 1: Why File Router?
- **English Answer:** The File Router acts as an intelligent decision engine at the single entry point. It inspects the physical file size on disk before loading and selects either a lightweight native Python streaming engine or a distributed PySpark engine. This optimizes startup latency for small files and throughput for big files.
- **Arabic Explanation:** يعمل موجه الملفات كمحرك قرار ذكي عند نقطة الدخول الموحدة، حيث يفحص حجم الملف على القرص قبل البدء، ويوجه الملف إما لمسار بايثون الخفيف أو لمسار Spark الموزع، مما يوفر سرعة إقلاع فورية للملفات الصغيرة وسرعة معالجة متوازية للملفات الضخمة.
- **Exact File / Function:** `src/file_router.py` -> `inspect_and_route()`
- **Why Selected:** To fulfill the single entry point requirement while maximizing hardware efficiency across heterogeneous file sizes.
- **Alternative:** Hardcoding a single engine (e.g., always running PySpark or always running Python).
- **Why Alternative Not Selected:** Running PySpark for small files incurs a 15–20s JVM boot penalty; running pure Python on 12.65 GB files risks single-core bottlenecking and slow ingestion.

---

## Question 2: Why was 200 MB chosen as the threshold?
- **English Answer:** 200 MB is the empirical inflection point where the execution speedup from multi-core PySpark parallelism surpasses the ~15–20 second overhead of initializing the JVM, SparkContext, and catalog.
- **Arabic Explanation:** 200 ميجابايت هي نقطة التوازن العملية التي تصبح عندها سرعة معالجة Spark الموزعة على أنوية متعددة أكبر من الوقت الضائع في إقلاع الـ JVM وتهيئة Spark (15 إلى 20 ثانية).
- **Exact File / Function:** `config/settings.py` -> `FILE_SIZE_THRESHOLD_MB = 200.0`
- **Why Selected:** Balances rapid interactive execution for operational batches with high-throughput distributed processing for bulk dumps.
- **Alternative:** Setting the threshold at 10 MB or 2 GB.
- **Why Alternative Not Selected:** 10 MB launches Spark unnecessarily for tiny files; 2 GB would process large gigabyte files on a single Python thread slowly.

---

## Question 3: Why Python Streaming?
- **English Answer:** Standard CSV reading (`pd.read_csv()` or `list(reader)`) loads all rows into RAM at once, causing memory spikes. Python streaming reads line-by-line in constant $O(1)$ memory using generator yields, flushing fixed-size chunks to MongoDB.
- **Arabic Explanation:** قراءة CSV التقليدية تحمل جميع السجلات في الذاكرة دفعة واحدة مما يسبب استهلاكاً هائلاً لـ RAM. القراءة بالتدفق تقرأ سطراً بسطر باستهلاك ذاكرة ثابت $O(1)$ وترسل دفعات محددة لـ MongoDB.
- **Exact File / Function:** `src/batch_loader.py` -> `stream_csv_batches()`
- **Why Selected:** Guarantees absolute memory safety (<50 MB RAM) on single-node execution.
- **Alternative:** In-memory batching using Pandas DataFrame chunks.
- **Why Alternative Not Selected:** Pandas allocates heavy internal DataFrame objects, causing high memory overhead.

---

## Question 4: Why PySpark for 12.65 GB?
- **English Answer:** The 12.65 GB dataset contains 30,000,000 records. PySpark splits the file across 99 distributed partitions, utilizing all 8 CPU cores in parallel to ingest 30M rows into MongoDB in just 10.4 minutes (~47,915 rows/s).
- **Arabic Explanation:** الملف بحجم 12.65 جيجابايت يحتوي على 30 مليون سجل. استخدام PySpark مكّن النظام من تقسيم الملف إلى 99 جزءاً متوازياً واستغلال جميع أنوية المعالج الثمانية لكتابة 30 مليون سجل في 10.4 دقيقة فقط بمعدل 47,915 سجل/ثانية.
- **Exact File / Function:** `src/spark_loader.py` -> `load_csv_to_raw_spark()`
- **Why Selected:** Industry-standard distributed computing engine designed for out-of-core parallel I/O.
- **Alternative:** Multi-threaded Python CSV reader.
- **Why Alternative Not Selected:** Python's Global Interpreter Lock (GIL) and single-node I/O bottlenecks limit multi-threading scalability on 12 GB+ files.

---

## Question 5: Why DataFrame API over RDDs?
- **English Answer:** The PySpark DataFrame API uses the Catalyst Optimizer and Tungsten execution engine for off-heap memory management and optimized binary serialization, avoiding expensive Python-JVM object conversions present in RDDs.
- **Arabic Explanation:** توفر DataFrame API تحسينات Catalyst Optimizer ومحرك Tungsten لإدارة الذاكرة والتسلسل الثنائي الأمثل، مما يتفوق بمراحل على RDDs التي تعاني من بطء التحويل بين Python و JVM.
- **Exact File / Function:** `src/spark_loader.py` -> `spark.read.csv()`
- **Why Selected:** Superior throughput, optimized execution plans, and native connector compatibility.
- **Alternative:** Low-level `sc.textFile()` and RDD transformations.
- **Why Alternative Not Selected:** Up to 5x slower due to Python serialization overhead on 30 million objects.

---

## Question 6: Why explicit String schema instead of `inferSchema`?
- **English Answer:** `inferSchema` scans the dataset twice, doubling disk I/O, and automatically coerces dirty tokens (e.g. `"???"`, Arabic numerals) into `null`, destroying raw fidelity. An explicit schema with all fields as `StringType` captures raw values exactly as written.
- **Arabic Explanation:** استخدام `inferSchema` يقرأ الملف مرتين ويقوم بتحويل القيم غير النظيفة (مثل النصوص والأرقام العربية) إلى `null` مما يدمر البيانات الأصلية. فرض Schema صريحة بنوع `StringType` يضمن وصول كل حرف كما هو إلى الـ Raw Layer.
- **Exact File / Function:** `src/spark_loader.py` -> `EXPLICIT_CSV_SCHEMA`
- **Why Selected:** 100% data fidelity preservation in ELT Raw Layer and 50% faster Spark read times.
- **Alternative:** `spark.read.option("inferSchema", "true")`.
- **Why Alternative Not Selected:** Destroys dirty data by replacing corrupted text with nulls and adds an extra full-file scan.

---

## Question 7: Why `utf-8-sig` encoding?
- **English Answer:** CSV files generated on Windows systems often contain a 3-byte Byte Order Mark (BOM: `\xef\xbb\xbf`) at the start of the file. Standard `utf-8` decodes it into `\ufefforder_id`, corrupting the first column name. `utf-8-sig` strips the BOM cleanly.
- **Arabic Explanation:** الملفات الناتجة من بيئات ويندوز تبدأ بعلامة BOM (`\xef\xbb\xbf`). قراءتها بـ `utf-8` العادي تجعل اسم العمود الأول ملوثاً بـ `\ufefforder_id`، بينما `utf-8-sig` يحذف الـ BOM تلقائياً ويقرأ اسم الحقل `order_id` بنظافة.
- **Exact File / Function:** `src/batch_loader.py` -> `encoding="utf-8-sig"`
- **Why Selected:** Robust handling of Arabic e-commerce CSVs with zero header corruption.
- **Alternative:** Manually stripping `\ufeff` from dictionary keys after parsing.
- **Why Alternative Not Selected:** Fragile, manual post-processing that is prone to edge-case bugs.

---

## Question 8: Why Raw Layer (`orders_raw`)?
- **English Answer:** The Raw Layer acts as an immutable, audited staging lake. It stores 100% of incoming data exactly as received with run metadata (`id_run`, `source_file`, `source_row_number`), ensuring that raw data is never lost even if downstream business rules change.
- **Arabic Explanation:** تمثل طبقة Raw بحيرة بيانات غير قابلة للتعديل تحفظ كل البيانات الواردة بدقة 100% مع بيانات التتبع (`id_run`، ورقم السطر)، مما يضمن عدم ضياع أي بيان أصلي حتى لو تم تغيير قواعد التنظيف لاحقاً.
- **Exact File / Function:** `src/mongo_setup.py` -> `COLL_RAW = "orders_raw"`
- **Why Selected:** Core tenet of Modern Data Architecture (ELT) and data lakehouse designs.
- **Alternative:** Transforming directly into the validated collection without a raw staging collection.
- **Why Alternative Not Selected:** Irreversible data loss if cleaning rules contain bugs or require retrospective reprocessing.

---

## Question 9: Why ELT instead of ETL?
- **English Answer:** In ETL, transformation precedes loading; unparseable rows are dropped or corrupted before persistence. In ELT, raw records are loaded first (Extract -> Load), and cleaning/transformation (Transform) happens downstream. This enables decoupled processing, full traceability, and reproducible corrections.
- **Arabic Explanation:** في ETL يتم تنظيف البيانات قبل تخزينها مما يتسبب في فقدان السجلات التالفة للأبد. في ELT نُحمّل البيانات أولاً ثم نُجري التحويل والتنظيف لاحقاً، مما يتيح التتبع الكامل وإمكانية إعادة المعالجة في أي وقت.
- **Exact File / Function:** `src/main.py` -> `run_pipeline()`
- **Why Selected:** High reliability, auditable data engineering, and zero data loss architecture.
- **Alternative:** Classical ETL pipeline discarding invalid records in memory.
- **Why Alternative Not Selected:** Impossible to audit what was dropped, violating enterprise data governance.

---

## Question 10: Explain each Rule (RULE_01 to RULE_09)
- **English Answer:**
  1. `RULE_01`: Normalizes Eastern/Arabic digits (`٠-٩`) and decimal separators (`٫٬`) to standard Latin digits (`0-9.,`).
  2. `RULE_02`: Extracts currency substrings ("ريال يمني", "YER", "USD", "SAR") and standardizes currency to `YER`.
  3. `RULE_03`: Removes thousands separator commas from numeric strings (`1,000` -> `1000`).
  4. `RULE_04`: Maps known Arabic number words ("ألف", "خمسة آلاف") to float equivalents.
  5. `RULE_05`: Cleans and standardizes 9-digit Yemeni phone numbers (prefixes 70, 71, 73, 77, 78).
  6. `RULE_06`: Fixes repeated email symbols (`@@` -> `@`, `..` -> `.`) and quarantines missing `@` or domain.
  7. `RULE_07`: Normalizes dates to ISO 8601 `YYYY-MM-DDTHH:MM:SS` and quarantines impossible dates.
  8. `RULE_08`: Trims whitespace and normalizes Yemeni city and order status synonyms.
  9. `RULE_09`: Validates JSON items array, parses line items safely, and recalculates order total = $\sum(\text{items}) + \text{delivery}$.
- **Arabic Explanation:**
  1. `RULE_01`: تحويل الأرقام العربية المشرقية `٠-٩` والفواصل إلى أرقام لاتينية `0-9`.
  2. `RULE_02`: استخراج نصوص العملات وتوحيد العملة إلى `YER`.
  3. `RULE_03`: حذف فواصل الآلاف من القيم النقدية.
  4. `RULE_04`: تحويل الكلمات الرقمية العربية ("ألف", "عشرة آلاف") إلى قيم عددية.
  5. `RULE_05`: تنظيف وتوحيد أرقام الهواتف اليمنية المكونة من 9 أرقام (70, 71, 73, 77, 78).
  6. `RULE_06`: إصلاح الرموز المكررة في البريد الإلكتروني (`@@` و `..`) وعزل البريد الفاقد للرمز `@`.
  7. `RULE_07`: توحيد التواريخ إلى صيغة ISO 8601 وعزل التواريخ المستحيلة أو الشاذة.
  8. `RULE_08`: إزالة المسافات وتوحيد ترادفات المدن اليمنية وحالات الطلبات.
  9. `RULE_09`: التحقق من مصفوفة عناصر الطلب JSON وإعادة حساب الإجمالي = مجموع العناصر + التوصيل.
- **Exact File / Function:** `src/quality_rules.py` -> Lines 95–520
- **Why Selected:** Deterministic, testable, business-aligned data sanitization.
- **Alternative:** Heuristic regexes or non-deterministic ML text cleaners.
- **Why Alternative Not Selected:** Non-deterministic algorithms introduce unpredictable errors in transactional e-commerce data.

---

## Question 11: Why Audit Trail?
- **English Answer:** The Audit Trail captures every modification made to a document inside a structured `corrections` array containing `rule_code`, `field`, `original_value`, `corrected_value`, and `reason`. This satisfies compliance, explains corrections, and allows data lineage tracking.
- **Arabic Explanation:** يوثق سجل التدقيق (Audit Trail) كل تغيير تم على البيانات داخل مصفوفة `corrections` موضحاً كود القاعدة، اسم الحقل، القيمة الأصلية، القيمة المصححة، وسبب التعديل، مما يحقق الشفافية التامة وتتبع أصل البيانات.
- **Exact File / Function:** `src/quality_rules.py` -> `process_and_classify_record()`
- **Why Selected:** Production compliance and transparent data observability.
- **Alternative:** Overwriting fields silently in-place without logging history.
- **Why Alternative Not Selected:** Destroys provenance; engineers cannot debug why a customer name or total changed.

---

## Question 12: Valid vs Corrected vs Quarantined?
- **English Answer:**
  - `Valid`: Clean raw record requiring zero modifications (`corrections == []`, `error_codes == []`).
  - `Corrected`: Record with minor formatting defects successfully repaired by deterministic rules (`len(corrections) > 0`, `error_codes == []`).
  - `Quarantined`: Record with critical unrecoverable defects (missing ID, corrupted JSON, impossible date) isolated in `orders_quarantine` (`len(error_codes) > 0`).
- **Arabic Explanation:**
  - `Valid`: سجل سليم 100% لم يحتج لأي تعديل.
  - `Corrected`: سجل به عيوب شكلية بسيطة تم إصلاحها وتوثيقها في سجل التدقيق.
  - `Quarantined`: سجل به تلف جوهري لا يمكن إصلاحه حتمياً (مثل فقدان رقم الطلب أو تلف JSON) وتم عزله.
- **Exact File / Function:** `src/quality_rules.py` -> Lines 634–685
- **Why Selected:** Strict 3-way mutually exclusive categorization of data quality.
- **Alternative:** Binary Valid/Invalid classification.
- **Why Alternative Not Selected:** Binary grouping loses the distinction between pure data and successfully repaired data.

---

## Question 13: Why MongoDB?
- **English Answer:** MongoDB provides native JSON document modeling that naturally matches the hierarchical nested schema of e-commerce orders (`items` array, `corrections` array), and offers high-throughput bulk upserts with unique secondary indexing.
- **Arabic Explanation:** توفر MongoDB نموذج وثائق JSON مرن يتطابق تماماً مع طبيعة بيانات الطلبات المتداخلة (مصفوفة العناصر `items` ومصفوفة التدقيق `corrections`)، مع دعم عمليات الإدراج والتحديث الجماعية فائقة السرعة.
- **Exact File / Function:** `src/mongo_setup.py`
- **Why Selected:** Native support for nested semi-structured arrays and high-performance WiredTiger engine.
- **Alternative:** Relational database (e.g. PostgreSQL or MySQL).
- **Why Alternative Not Selected:** Requires multi-table normalization (Orders, OrderItems, Corrections) with expensive multi-table joins during streaming ingestion.

---

## Question 14: Why `id_order` as Business Key?
- **English Answer:** `id_order` is the domain-level unique identifier for an e-commerce transaction. Using `id_order` as the unique business key allows the system to recognize duplicate messages, modifications, and updates across repeated ingestion runs.
- **Arabic Explanation:** يمثل `id_order` المعرف الطبيعي للمعاملة التجارية. اعتماده كمفتاح أعمال يتيح للنظام التعرف على تكرار السجلات وتحديثها بدلاً من تكرارها عند إعادة تشغيل الـ Pipeline.
- **Exact File / Function:** `config/settings.py` -> `BUSINESS_KEY = "id_order"`
- **Why Selected:** Provides natural entity identity and idempotency.
- **Alternative:** Using surrogate database autoincrement integer keys or MongoDB `ObjectId`.
- **Why Alternative Not Selected:** Surrogate keys treat repeated ingestion of the same order as a brand new entity, causing silent duplicate records.

---

## Question 15: How does Unique Index work?
- **English Answer:** A unique index (`uniq_id_order`) is built on `orders_validated.id_order` with a B-tree structure. The database engine enforces uniqueness at the storage layer, rejecting duplicate inserts and enabling $O(\log N)$ lookup during upsert operations.
- **Arabic Explanation:** ينشئ الفهرس الفريد شجرة B-Tree على حقل `id_order`، ويفرض محرك التخزين عدم التكرار على مستوى قاعدة البيانات، مما يمنع إدخال سجلين بنفس المفتاح ويوفر سرعة بحث فائقة $O(\log N)$.
- **Exact File / Function:** `src/mongo_setup.py` -> `create_index("uniq_id_order", unique=True)`
- **Why Selected:** Database-level constraint guaranteeing data integrity even under concurrent writes.
- **Alternative:** Checking for duplicates in Python application memory before writing.
- **Why Alternative Not Selected:** Vulnerable to race conditions and requires massive memory to hold all known keys.

---

## Question 16: How does `UpdateOne(..., upsert=True)` provide idempotent behavior?
- **English Answer:** `UpdateOne({"id_order": doc["id_order"]}, {"$set": doc}, upsert=True)` checks the collection for an existing document matching `id_order`. If not found, it inserts it (`upserted_id`). If found, it updates fields in-place (`modified_count`). Re-running the same record produces zero new documents.
- **Arabic Explanation:** تقوم التعليمة بفحص وجود رقم الطلب: إذا لم يكن موجوداً تقوم بإدراجه، وإذا كان موجوداً تقوم بتحديث بياناته في مكانه. تشغيل نفس البيانات 100 مرة يعطي نفس النتيجة تماماً دون زيادة عدد الوثائق.
- **Exact File / Function:** `src/elt_pipeline.py` -> `flush_validated_batch()`
- **Why Selected:** Atomic, database-native idempotent upsert operation.
- **Alternative:** `insert_many(..., ordered=False)` with duplicate key error catching.
- **Why Alternative Not Selected:** `insert_many` fails to update modified fields on updated orders and floods logs with duplicate key exceptions.

---

## Question 17: Explain Inserted vs Updated vs Unchanged.
- **English Answer:**
  - `Inserted`: Document with a new business key inserted for the first time (`upserted_id` returned by MongoDB).
  - `Updated`: Existing document where raw fields changed, causing an in-place field update (`matched_count > 0` and `modified_count > 0`).
  - `Unchanged`: Existing document where the new data is identical to current DB state (`matched_count > 0` and `modified_count == 0`).
- **Arabic Explanation:**
  - `Inserted`: وثيقة جديدة أُدخلت لأول مرة بقاعدة البيانات.
  - `Updated`: وثيقة موجودة مسبقاً طرأ تعديل على بياناتها فتم تحديث حقولها في مكانها.
  - `Unchanged`: وثيقة موجودة مسبقاً والبيانات الواردة مطابقة تماماً للمخزن فلم يتغير شيء.
- **Exact File / Function:** `src/elt_pipeline.py` -> `flush_validated_batch()`
- **Why Selected:** Comprehensive tracking of database mutation states.
- **Alternative:** Counting total bulk write operations as simple inserts.
- **Why Alternative Not Selected:** Fails to distinguish between new entity creation and duplicate entity updates.

---

## Question 18: Why are duplicates updated rather than quarantined?
- **English Answer:** In transactional e-commerce, repeated order IDs represent order status updates (e.g. `قيد الانتظار` -> `تم التسليم`). Updating existing business records in `orders_validated` reflects real-world domain entity state convergence rather than data corruption.
- **Arabic Explanation:** في التجارة الإلكترونية، تكرار رقم الطلب يمثل تحديثاً لحالة الطلب (مثل التحول من 'قيد الشحن' إلى 'تم التسليم'). تحديث الوثيقة يعكس الحالة الأحدث للطلب، بينما عزلها يحرم النظام من معرفة الحالة المحدثة.
- **Exact File / Function:** `src/elt_pipeline.py` -> `flush_validated_batch()`
- **Why Selected:** Follows domain-driven design and idempotent state convergence.
- **Alternative:** Moving all duplicate order IDs into `orders_quarantine`.
- **Why Alternative Not Selected:** Corrupts business analytics by treating valid status updates as bad data.

---

## Question 19: Explain `source_row_number` in Python vs PySpark.
- **English Answer:** In Python Batch, `source_row_number` is the exact 1-indexed sequential line number in the CSV. In PySpark, it is a globally unique 64-bit distributed integer from `monotonically_increasing_id() + 1` (where partition index is stored in the upper 33 bits), avoiding expensive cross-node sorting.
- **Arabic Explanation:** في مسار بايثون يمثل رقم السطر الفعلي في ملف CSV. أما في Spark فهو معرّف رقمي فريد وموزع يُولد عبر `monotonically_increasing_id() + 1` حيث تُحفظ هوية الـ Partition في البتات العليا لتفادي عمليات الفرز الموزعة البطيئة جداً.
- **Exact File / Function:** `src/batch_loader.py` (Line 60) & `src/spark_loader.py` (Line 132)
- **Why Selected:** Guarantees line traceability on small files and distributed performance on Spark.
- **Alternative:** Using `zipWithIndex` in Spark to force physical row numbering.
- **Why Alternative Not Selected:** Forces global serialization and massive network shuffle across all executors.

---

## Question 20: Explain the Consistency Equation.
- **English Answer:** $\text{Raw Count} = \text{Valid Count} + \text{Corrected Count} + \text{Quarantined Count}$. Every raw document entering the pipeline must be definitively accounted for in exactly one category. If the sum does not balance, an exception is thrown.
- **Arabic Explanation:** معادلة توازن حسابية تضمن عدم ضياع أي سجل: إجمالي سجلات Raw يجب أن يتطابق بنسبة 100% مع مجموع (السليمة + المصححة + المعزولة). إذا اختلف سجل واحد يتوقف البرنامج ويطلق استثناءً فورياً.
- **Exact File / Function:** `src/elt_pipeline.py` -> Lines 216–228
- **Why Selected:** Mathematical proof of zero record loss and data integrity enforcement.
- **Alternative:** Logging record counts without strict assertion check.
- **Why Alternative Not Selected:** Silent record drops could slip through undetected.

---

## Question 21: Explain the 99 Spark partitions for the 12.65 GB file.
- **English Answer:** Spark divides CSV files into default 128 MB blocks. $\frac{12,650.32\text{ MB}}{128\text{ MB}} \approx 98.83$, resulting in exactly 99 parallel partitions. This allowed Spark to distribute the 30 million rows evenly across executor cores.
- **Arabic Explanation:** يقسم محرك Spark الملفات تلقائياً إلى كتل بحجم 128 ميجابايت. قسمة 12,650 ميجابايت على 128 ميجابايت تعطي 99 Partition، مما أتاح توزيع الـ 30 مليون سجل بالتساوي على أنوية المعالج.
- **Exact File / Function:** `src/spark_loader.py` -> `df.rdd.getNumPartitions()`
- **Why Selected:** Optimal memory partitioning without single-partition skew or executor memory exhaustion.
- **Alternative:** Coalescing into a single partition (`df.repartition(1)`).
- **Why Alternative Not Selected:** Forces all 12.65 GB onto a single core, causing Out-Of-Memory crashes.

---

## Question 22: Explain the MongoDB Spark Connector setup.
- **English Answer:** We utilized the official `mongo-spark-connector_2.13:10.4.0` (matching Spark 3.5.0, Scala 2.13, Java 17, and MongoDB 7.0). Local project-relative JARs are supplied to `spark.jars` to eliminate Windows `winutils.exe` Ivy download bugs.
- **Arabic Explanation:** استخدمنا الموصل الرسمي `mongo-spark-connector_2.13:10.4.0` المتوافق مع Spark 3.5 و Scala 2.13 و Java 17. وضع مكتبات JAR محلياً في مجلد المشروع ألغى الحاجة لـ `winutils.exe` وضمن استقرار التشغيل على بيئة ويندوز.
- **Exact File / Function:** `src/spark_loader.py` -> `get_spark_classpath()`
- **Why Selected:** Battle-tested connector supporting distributed parallel bulk partition writes.
- **Alternative:** Converting Spark partitions to Pandas and writing via PyMongo loops.
- **Why Alternative Not Selected:** Severe driver bottleneck; 10x slower than direct connector writes.

---

## Question 23: Explain memory-safe batching in the ELT streaming stage.
- **English Answer:** The ELT pipeline uses a streaming MongoDB cursor and fixed-size buffers (`batch_size = 10,000`). It accumulates processed documents, flushes them via `bulk_write`, and immediately clears the Python list, keeping memory flat at $O(1)$ throughout all 30 million records.
- **Arabic Explanation:** يستخدم خط الـ ELT مؤشر تدفق من MongoDB مع ذاكرة وسيطة بحجم ثابت (10,000 وثيقة). تتم معالجة الدفعة وكتابتها بـ `bulk_write` وتفريغ القائمة فوراً من الذاكرة، مما حافظ على ثبات RAM طوال معالجة الـ 30 مليون سجل.
- **Exact File / Function:** `src/elt_pipeline.py` -> `flush_validated_batch()` & `flush_quarantine_batch()`
- **Why Selected:** Prevents Out-Of-Memory crashes regardless of collection scale.
- **Alternative:** Appending all 30M records to an in-memory list before writing.
- **Why Alternative Not Selected:** Consumes over 35 GB of RAM, causing system crash on 16 GB machines.

---

## Question 24: Explain the MongoDB storage incident and how it was resolved.
- **English Answer:** In initial execution, MongoDB was installed on the `C:` drive (which had only 14 GB free). Ingesting 30M raw documents plus indexes consumed ~13.5 GB, risking disk exhaustion. We migrated MongoDB's data directory and WiredTiger journal safely to drive `D:\MongoDB\data` (which had 35 GB free), ensuring a safe >15 GB headroom throughout.
- **Arabic Explanation:** عند بدء التشغيل كانت قاعدة البيانات على القرص `C:` الذي يملك 14GB فقط، واستهلكت السجلات 13.5GB مما كاد يسبب امتلاء القرص. قمنا بنقل مسار التخزين بأمان إلى القرص `D:\MongoDB\data` الذي يملك أكثر من 35GB، مما حافظ على هامش أمان مستقر (>15GB) طوال فترة التشغيل.
- **Exact File / Function:** `D:\MongoDB\mongod.cfg` -> `storage.dbPath: "D:\\MongoDB\\data"`
- **Why Selected:** Eliminates disk space constraints and protects OS system drive stability.
- **Alternative:** Reducing batch sizes or deleting indexes.
- **Why Alternative Not Selected:** Deleting indexes ruins query/upsert performance; moving storage solves the root physical constraint.

---

## Question 25: Explain the RFC-4180 quote and escape parser fix.
- **English Answer:** The dataset's `items_json` column contains nested JSON with escaped quotes (`""sku"":""SKU-1010""`). Default Spark CSV readers split on inner quotes unless explicitly configured. Adding `.option("quote", """)` and `.option("escape", """)` achieved 100% parity with Python's standard `csv.DictReader`.
- **Arabic Explanation:** يحتوي عمود `items_json` على نصوص JSON بداخلها علامات تنصيص مقتبسة (`""sku""`). قارئ Spark الافتراضي قد يفسرها بشكل خاطئ ما لم يتم تفعيل خيارات الاقتباس والهروب القياسية (`quote` و `escape`)، مما حقق تطابقاً تاماً بنسبة 100% مع قارئ بايثون.
- **Exact File / Function:** `src/spark_loader.py` -> Lines 124–125
- **Why Selected:** Compliance with RFC-4180 CSV standard for nested JSON payloads.
- **Alternative:** Custom regex pre-processing on the 12.65 GB file.
- **Why Alternative Not Selected:** Expensive extra disk I/O pass; built-in parser options solve it natively.

---

## Question 26: Explain why the project uses YER and +967.
- **English Answer:** The e-commerce dataset models the Yemeni retail market, featuring Yemeni Rial (`YER`) prices, Yemeni governorates (Sana'a, Aden, Taiz, Ibb, Mukalla), and Yemeni telecommunication mobile operators (`70`, `71`, `73`, `77`, `78` with country code `+967`). The pipeline is tailored to normalize these specific domain patterns.
- **Arabic Explanation:** يمثل ملف البيانات سوق التجزئة اليمني، بأسعار بالريال اليمني (`YER`) ومدن ومحافظات يمنية (صنعاء، عدن، تعز، إب، المكلا) وأرقام هواتف تتبع شبكات الاتصال اليمنية (`70`, `71`, `73`, `77`, `78` بمفتاح دولي `+967`).
- **Exact File / Function:** `src/quality_rules.py` -> `KNOWN_CITIES`, `normalize_phone`, `normalize_currency_and_text`
- **Why Selected:** Aligned with actual dataset schema and regional business rules.
- **Alternative:** Hardcoding international generic phone numbers.
- **Why Alternative Not Selected:** Fails to validate regional 9-digit mobile patterns accurately.

---

## Question 27: Why does RULE_08 have count 0 in the 30M results?
- **English Answer:** RULE_08 (Categorical trim and synonym normalization) is fully implemented in code. In the synthetic 30M dataset, text fields (`city`, `status`) were already generated using canonical values, and numeric substrings were cleaned by RULE_01, so no categorical values triggered synonym re-mapping.
- **Arabic Explanation:** القاعدة RULE_08 مطبقة ومفعلة بالكامل في الكود. في ملف البيانات المولد (30 مليون سجل)، كانت حقول المدن والحالات مطابقة مسبقاً للقيم القياسية، فتمت معالجتها دون الحاجة لتسجيل تصحيح ترادفي منفصل في سجل التدقيق.
- **Exact File / Function:** `src/quality_rules.py` -> `normalize_text_and_synonyms()`
- **Why Selected:** Safe pass-through behavior when data is already canonical.
- **Alternative:** Artificially forcing dummy corrections to show a non-zero count.
- **Why Alternative Not Selected:** Dishonest engineering; metrics must strictly reflect true data state.

---

## Question 28: Explain the 30M execution metrics and timeline.
- **English Answer:**
  - **PySpark Load (Stage 0):** 30,000,000 raw rows loaded in 626.106s (~10.43 mins) at **47,915.21 rows/s**.
  - **Streaming ELT (Stage 1):** 30,000,000 rows transformed and upserted in 14,866.692s (~4.13 hours) at **2,017.93 rows/s**.
  - **Total Pipeline Execution Time:** 15,493.115s (~**4.30 hours**) at an overall throughput of **1,936.34 rows/s**.
- **Arabic Explanation:**
  - **مرحلة تحميل Spark:** 30 مليون سجل تم استيرادها في 10.43 دقيقة بمعدل **47,915 سجل/ثانية**.
  - **مرحلة الـ ELT والـ Upsert:** 30 مليون سجل تم تنظيفها وإدراجها في 4.13 ساعة بمعدل **2,017 سجل/ثانية**.
  - **إجمالي وقت التشغيل:** 4.30 ساعات بإنتاجية كلية بلغت **1,936 سجل/ثانية**.
- **Exact File / Function:** `reports/results.json` & `reports/results.md`
- **Why Selected:** Empirical evidence of production-grade scalability on a 12.65 GB dataset.
- **Alternative:** Estimating theoretical speed without executing full dataset.
- **Why Alternative Not Selected:** True empirical proof demonstrates system robustness under real multi-hour workloads.

---

## Question 29: Explain the difference between processing count (28,320,907) and collection count (28,121,300).
- **English Answer:** 28,320,907 is the number of row-level operations ($23,976,182\text{ Valid} + 4,344,725\text{ Corrected}$). Because 199,607 rows shared existing `id_order` keys, the idempotent upsert updated them in-place, leaving exactly 28,121,300 unique business entity documents in `orders_validated`.
- **Arabic Explanation:** 28,320,907 يمثل عدد عمليات معالجة السجلات الصالحة والمصححة على مستوى الصفوف. وبسبب وجود 199,607 سجل مكرر بنفس رقم الطلب، قام الـ Upsert بتحديثها في نفس الوثائق دون زيادة عددها، فاستقر عدد الوثائق الفريدة في `orders_validated` عند 28,121,300 وثيقة.
- **Exact File / Function:** `src/elt_pipeline.py` -> `upsert_counts`
- **Why Selected:** Distinguishes row-level pipeline throughput from collection-level entity cardinality.
- **Alternative:** Confusing row operations with distinct document counts.
- **Why Alternative Not Selected:** Leads to apparent contradictions during technical data audits.

---

## Question 30: Why was 30M Idempotency not re-tested in Run 2?
- **English Answer:** Idempotency is an algorithmic and architectural property of `UpdateOne(..., upsert=True)` and unique indexing. It was experimentally verified on the 100K test dataset (0 new inserts on Run 2). Re-running the 30M dataset would have required an additional 4.3 hours without providing any new architectural insight.
- **Arabic Explanation:** خاصية الـ Idempotency هي خاصية معمارية وخوارزمية مثبتة في كود الـ Upsert والفهرس الفريد. تم إثباتها عملياً وتجريبياً على عينة 100K (نتج عنها صفر وثائق جديدة في Run 2). إعادة تشغيل الـ 30M مجدداً كانت ستستغرق 4.3 ساعات إضافية دون تقديم أي إضافة معمارية جديدة.
- **Exact File / Function:** `docs/FINAL_VERIFICATION.md` & `tests/run_verification.py`
- **Why Selected:** Transparent and honest academic reporting adhering to resource constraints.
- **Alternative:** Claiming that 30M Run 2 was executed when it was not.
- **Why Alternative Not Selected:** Falsifying verification evidence violates academic integrity.
