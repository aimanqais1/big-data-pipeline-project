# Comprehensive Viva Examination & Defense Guide
**Course:** Big Data – Practical (Midterm Examination)  
**Project:** Hybrid Data Engineering Pipeline for E-Commerce Orders  
**Target:** Individual Student Oral Defense

---

## Question 1: What is the fundamental difference between ETL and ELT, and why did you choose ELT?
- **Short English Answer:** In ETL, transformations happen before loading, which risks permanent data loss on dirty records. In ELT, raw data is loaded first with 100% fidelity into `orders_raw`, and transformations occur downstream, ensuring full auditability and repeatability.
- **Arabic Explanation:** في ETL يتم تنظيف البيانات قبل تخزينها، وإذا كان هناك خطأ في كود التنظيف فإن البيانات الأصلية تُفقد نهائياً. في ELT نُحمّل البيانات كما هي أولاً في `orders_raw` للحفاظ على القيمة الأصلية، ثم نُجري التنظيف والتصنيف لاحقاً.
- **Relevant File:** `src/elt_pipeline.py` & `src/batch_loader.py`
- **Relevant Function:** `load_csv_to_raw_batch()` & `run_elt_transform_and_classify()`
- **Why We Used This Design:** To guarantee zero data loss and enable re-processing raw records whenever cleaning rules evolve.
- **Alternative:** ETL (Cleaning during ingestion).
- **Why Not Used:** Drops or corrupts raw dirty records prematurely before database persistence.

---

## Question 2: Why do you have a File Router, and why was 200 MB chosen as the threshold?
- **Short English Answer:** The File Router selects the optimal engine to avoid Spark's JVM startup overhead (15–20s) on small files while using Spark's distributed partitions for large files exceeding single-node memory. 200 MB represents the trade-off point where Spark's parallelism outpaces Python's overhead.
- **Arabic Explanation:** لتفادي تكلفة إقلاع بيئة Spark و JVM (تستغرق 15-20 ثانية) على الملفات الصغيرة حيث يعمل مفسر بايثون بسرعة فائقة، بينما في الملفات الأكبر من 200MB تتفوق قدرة Spark على توزيع البيانات على عدة أنوية ومعالجة البيانات التي تفوق حجم الذاكرة.
- **Relevant File:** `src/file_router.py` & `config/settings.py`
- **Relevant Function:** `inspect_and_route()`
- **Why We Used This Design:** Single entry point requirement and optimal resource utilization.
- **Alternative:** Running PySpark unconditionally for all file sizes.
- **Why Not Used:** Causes massive, unnecessary latency on small operational batches.

---

## Question 3: Why cannot you use `list(reader)` or `pd.read_csv()` for the ingestion?
- **Short English Answer:** `list(reader)` and Pandas load the entire dataset into memory simultaneously, causing Out-Of-Memory (OOM) crashes on large files (e.g., 12.6 GB). Our streaming generator achieves constant $O(1)$ memory usage regardless of dataset size.
- **Arabic Explanation:** قراءة الملف كاملاً في الذاكرة عبر Pandas أو `list(reader)` تؤدي إلى نفاد الذاكرة العشوائية (OOM) وانهيار النظام عند معالجة الملفات الضخمة. استخدام الـ Generator Streams يجعل استهلاك الذاكرة ثابتاً $O(1)$ (< 50 MB).
- **Relevant File:** `src/batch_loader.py` & `src/create_small_sample.py`
- **Relevant Function:** `stream_csv_batches()`
- **Why We Used This Design:** Memory-safety and production scalability.
- **Alternative:** Pandas DataFrame chunks or in-memory list loading.
- **Why Not Used:** High memory footprint and crashes on 12.6 GB files.

---

## Question 4: Why did you use an Explicit Schema in Spark instead of `inferSchema`?
- **Short English Answer:** `inferSchema` requires an extra pass over the dataset and crashes or silently converts dirty values (e.g., `"???"` or Arabic numerals) into `null`. An explicit schema with `StringType` preserves the exact raw characters.
- **Arabic Explanation:** استخدام `inferSchema` يتطلب قراءة الملف مرتين ويقوم بتحويل القيم غير النظيفة (مثل النصوص في حقول الأرقام) إلى `null` مما يؤدي لتلف البيانات. فرض Schema ثابتة يضمن وصول كل رمز كما هو إلى الـ Raw Layer.
- **Relevant File:** `src/spark_loader.py`
- **Relevant Function:** `load_csv_to_raw_spark()` & `EXPLICIT_CSV_SCHEMA`
- **Why We Used This Design:** Data fidelity preservation and avoiding schema inference latency.
- **Alternative:** `spark.read.option("inferSchema", "true")`.
- **Why Not Used:** Corrupts dirty values and doubles disk I/O time.

---

## Question 5: Why are all fields defined as `StringType` in `orders_raw`?
- **Short English Answer:** Because raw data contains mixed-quality tokens (e.g. `"٧٠٦٠٠٠٫٠"`, `"125,000.00"`, `"5000 ريال"`). Enforcing numeric types in Raw would cause parser exceptions or data loss.
- **Arabic Explanation:** لأن البيانات الواردة ملوثة وتحتوي على أرقام عربية وفواصل ونصوص عملات، وقراءتها كأنواع رقمية في Raw سيتسبب في فشل القراءة أو تصفير القيم.
- **Relevant File:** `src/spark_loader.py` & `src/quality_rules.py`
- **Relevant Function:** `EXPLICIT_CSV_SCHEMA`
- **Why We Used This Design:** Preserves exact raw fidelity in the ELT Raw Layer.
- **Alternative:** Immediate casting to FloatType/TimestampType during ingestion.
- **Why Not Used:** Silently coerces unparseable values to `null`.

---

## Question 6: What is a Stable Business Key, and why is `id_order` used?
- **Short English Answer:** A Stable Business Key is an immutable natural domain identifier representing a single business entity. `id_order` identifies an order regardless of how many times it is transmitted or modified.
- **Arabic Explanation:** هو المفتاح الطبيعي الثابت الذي يمثل المعاملة التجارية الحقيقية. استخدام `id_order` يضمن التعرف على الطلب وتحديثه دون تكراره مهما تكرر إرسال الملف.
- **Relevant File:** `config/settings.py` & `src/mongo_setup.py`
- **Relevant Function:** `setup_mongodb_collections()`
- **Why We Used This Design:** Enforces entity stability and uniqueness in `orders_validated`.
- **Alternative:** Relying on MongoDB auto-generated `_id` (ObjectId).
- **Why Not Used:** Auto `_id` generates a new ID on every run, creating duplicate business records.

---

## Question 7: What is Idempotency, and how did you prove it in your project?
- **Short English Answer:** An operation is idempotent if executing it multiple times yields the exact same state without unintended side-effects. We proved it by running the 100K dataset in Run 1 (inserted 93,806 entities) and then executing Run 2 on the exact same dataset, resulting in **0 new inserts** and 0 duplicate documents in MongoDB.
- **Arabic Explanation:** أن تنفيذ العملية عدة مرات على نفس البيانات يعطي نفس النتيجة تماماً دون تكرار أو أخطاء. أثبتنا ذلك بتشغيل الـ Pipeline في Run 1 (أدخل 93,806 طلب فريد)، ثم تشغيل Run 2 بنفس البيانات فنتج عنه **صفر إدخالات جديدة** وثبات إجمالي السجلات في MongoDB تماماً.
- **Relevant File:** `src/elt_pipeline.py` & `tests/run_verification.py`
- **Relevant Function:** `flush_validated_batch()` & `execute_verification()`
- **Why We Used This Design:** Production data resilience against retry and duplicate message delivery.
- **Alternative:** Standard append `insert_many()`.
- **Why Not Used:** Duplicates every single document on every pipeline re-run.

---

## Question 8: Why did you use `UpdateOne(..., upsert=True)` instead of `insert_many`?
- **Short English Answer:** `insert_many` fails with duplicate key errors on unique indexes during re-runs. `upsert=True` seamlessly inserts non-existent records and updates existing records matching the business key.
- **Arabic Explanation:** لأن `insert_many` ينهار عند وجود Unique Index في حال تكرار السجل، بينما الـ Upsert يتحقق من وجود المفتاح: إذا كان جديداً يدخله، وإذا كان موجوداً يحدّثه بأمان.
- **Relevant File:** `src/elt_pipeline.py`
- **Relevant Function:** `flush_validated_batch()`
- **Why We Used This Design:** Native database-level idempotency and conflict resolution.
- **Alternative:** Querying for existing keys before inserting one-by-one.
- **Why Not Used:** Extremely slow ($O(N)$ round-trips) and prone to race conditions.

---

## Question 9: Why is there a Unique Index on `orders_validated`, but NOT on `orders_raw`?
- **Short English Answer:** `orders_raw` must accept all incoming raw lines without restriction to preserve historical audit logs. `orders_validated` represents clean business entities and must enforce unique business keys via `uniq_id_order`.
- **Arabic Explanation:** لأن `orders_raw` يجب أن تستقبل كل السجلات كما وردت حتى لو كانت مكررة أو تالفة للأرشفة والتدقيق، بينما `orders_validated` تمثل الكيانات التجارية النظيفة ويجب منع التكرار فيها عبر Unique Index.
- **Relevant File:** `src/mongo_setup.py`
- **Relevant Function:** `setup_mongodb_collections()`
- **Why We Used This Design:** Decouples raw ingestion logging from clean business domain constraints.
- **Alternative:** Putting unique indexes on Raw collection.
- **Why Not Used:** Prevents loading dirty or duplicate incoming raw batches.

---

## Question 10: What is Quarantine, and why not just drop bad records?
- **Short English Answer:** Dropping records silently causes data loss and prevents business reconciliation. Quarantine stores unfixable records in `orders_quarantine` with specific error codes and raw payloads for engineering inspection.
- **Arabic Explanation:** حذف السجلات التالفة بهدوء يخفي مشاكل الجودة ويمنع المحاسبة وتدقيق البيانات. العزل (Quarantine) يحتفظ بالسجلات الفاسدة مع أكواد الأخطاء والبيانات الأصلية لدراستها وتصحيحها لاحقاً.
- **Relevant File:** `src/elt_pipeline.py` & `src/quality_rules.py`
- **Relevant Function:** `process_and_classify_record()`
- **Why We Used This Design:** Total accountability and auditability for all data defects.
- **Alternative:** `df.dropna()` or skipping corrupted rows.
- **Why Not Used:** Unacceptable in production finance and e-commerce systems.

---

## Question 11: What is the Audit Trail, and what does it contain?
- **Short English Answer:** The Audit Trail is a granular record of all transformations applied to a document. It captures `rule_code`, `field`, `original_value`, `corrected_value`, and `reason` within the `corrections` array.
- **Arabic Explanation:** هو سجل تدقيق تفصيلي يوثق كل تعديل تم على السجل: ما هو الحقل، القيمة الأصلية، القيمة المصححة، كود القاعدة، وسبب التعديل.
- **Relevant File:** `src/quality_rules.py`
- **Relevant Function:** `process_and_classify_record()`
- **Why We Used This Design:** Compliance, debugging, and explainability.
- **Alternative:** Overwriting fields without tracking historical changes.
- **Why Not Used:** Impossible to know how or why a value was modified.

---

## Question 12: Explain the Strict Consistency Equation.
- **Short English Answer:** $\text{Raw Ingested} = \text{Valid} + \text{Corrected} + \text{Quarantined}$. Every record entering the pipeline must be definitively accounted for in exactly one category. If the sum does not match, a Data Integrity Error is raised.
- **Arabic Explanation:** معادلة حسابية صارمة تضمن عدم ضياع أي سجل: إجمالي سجلات Raw يجب أن يساوي مجموع (السليمة + المصححة + المعزولة) بنسبة 100%. إذا حدث أي اختلاف يتوقف النظام ويطلق خطأ في تكامل البيانات.
- **Relevant File:** `src/elt_pipeline.py`
- **Relevant Function:** `run_elt_transform_and_classify()`
- **Why We Used This Design:** Mathematical guarantee of zero record leakage.
- **Alternative:** Logging counts without asserting strict equality.
- **Why Not Used:** Allows silent record loss to pass unnoticed.

---

## Question 13: How does the Update Scenario demonstrate pipeline correctness?
- **Short English Answer:** By modifying a field (e.g. `customer_name`) in a raw record and re-running the ELT step, the pipeline updates the existing document in `orders_validated` without changing the total count of business entities in the collection.
- **Arabic Explanation:** بتعديل حقل في سجل موجود وإعادة تشغيل المعالجة، نثبت أن النظام يحدّث السجل المعني بدقة دون إضافة وثيقة مكررة مع بقاء إجمالي عدد الطلبات في قاعدة البيانات ثابتاً.
- **Relevant File:** `tests/run_verification.py`
- **Relevant Function:** `execute_verification()`
- **Why We Used This Design:** Proves that the pipeline correctly handles record modifications and updates.
- **Alternative:** Manual inspection of database.
- **Why Not Used:** Automated scripted verification is reproducible and verifiable.

---

## Question 14: What is the purpose of `utf-8-sig` encoding?
- **Short English Answer:** The raw dataset contains a UTF-8 Byte Order Mark (BOM). Standard `utf-8` decodes it as `\ufefforder_id`, corrupting the first dictionary key. `utf-8-sig` automatically strips the BOM cleanly.
- **Arabic Explanation:** الملف يبدأ بـ BOM (`\xef\xbb\xbf`). قراءته بـ `utf-8` العادي تجعل اسم العمود الأول `\ufefforder_id`، بينما `utf-8-sig` يتجاهل الـ BOM ويقرأ اسم العمود `order_id` بنظافة.
- **Relevant File:** `config/settings.py` & `src/batch_loader.py`
- **Relevant Function:** `stream_csv_batches()`
- **Why We Used This Design:** Prevents column header corruption.
- **Alternative:** Manually replacing `\ufeff` in string headers.
- **Why Not Used:** Fragile and non-standard.

---

## Question 15: How does `source_row_number` work in Python Batch vs PySpark?
- **Short English Answer:** In Python Batch, it is the exact physical sequential CSV line number ($1, 2, 3, \dots$). In PySpark, it is a globally unique 64-bit distributed identifier generated via `monotonically_increasing_id()` that avoids expensive cross-partition sorting.
- **Arabic Explanation:** في مسار بايثون يمثل رقم السطر الفعلي التسلسلي في الملف. أما في Spark فهو معرّف فريد وموزع على مستوى الـ Partitions يُولد عبر `monotonically_increasing_id()` لمنع عمليات الـ Shuffling البطيئة جداً.
- **Relevant File:** `src/batch_loader.py` & `src/spark_loader.py`
- **Relevant Function:** `load_csv_to_raw_batch()` & `load_csv_to_raw_spark()`
- **Why We Used This Design:** Balances physical traceability on small batches with distributed performance on Spark.
- **Alternative:** Using `zipWithIndex` in Spark to force physical row numbering.
- **Why Not Used:** Triggers expensive global distributed serialization and slows down Spark.

---

## Question 16: Why did you use local JARs for the MongoDB Spark Connector instead of Maven remote fetching?
- **Short English Answer:** On Windows, Spark's remote Ivy resolver calls Hadoop `winutils.exe` to `chmod` downloaded packages. Since `HADOOP_HOME` is unset on standard Windows, this fails. Using project-relative local JARs eliminates this dependency cleanly.
- **Arabic Explanation:** على نظام ويندوز، محاولة تحميل الحزم عبر Ivy تحاول استدعاء `winutils.exe` لتغيير صلاحيات الملفات، وفي حال عدم ضبط Hadoop تفشل العملية. وضع مكتبات الـ JARs محلياً يوفر استقراراً تاماً وتوافقية فورية.
- **Relevant File:** `config/settings.py` & `src/spark_loader.py`
- **Relevant Function:** `get_spark_classpath()`
- **Why We Used This Design:** Ensures 100% reproducible execution on Windows environments.
- **Alternative:** Installing full Apache Hadoop binary distribution and winutils.
- **Why Not Used:** Heavyweight, complex setup for an individual project.

---

## Question 17: Why are cleaning rules deterministic and not AI or Fuzzy based?
- **Short English Answer:** Financial and e-commerce data pipelines require exact mathematical determinism and predictability. Ambiguous values must be quarantined rather than guessed by non-deterministic models.
- **Arabic Explanation:** لأن البيانات المالية والتجارية تتطلب دقة حتمية 100%، والتخمين أو استخدام نماذج غير حتمية قد يولد أسعاراً أو أرقاماً خاطئة للعملاء. إذا كانت القيمة غير مؤكدة تُعزل فوراً.
- **Relevant File:** `src/quality_rules.py`
- **Relevant Function:** `validate_and_recalculate_order()`
- **Why We Used This Design:** High reliability, safety, and auditable data quality.
- **Alternative:** Fuzzy string matching / Machine learning imputers.
- **Why Not Used:** Dangerous in commercial billing and order processing systems.

---

## Question 18: What happens when an order total doesn't match the items sum?
- **Short English Answer:** Under RULE_09, if the items array and delivery cost are valid and trustworthy, the pipeline deterministically recalculates the total. If the items themselves are corrupt or ambiguous, the record is quarantined with `CORRUPTED_ITEMS_JSON` or `AMBIGUOUS_NEGATIVE_VALUE`.
- **Arabic Explanation:** وفق القاعدة RULE_09، إذا كانت العناصر وأسعارها صالحة وتكلفة التوصيل موجودة، يُعاد حساب الإجمالي بدقة ويسجل في الـ Audit Trail. أما إذا كانت العناصر نفسها تالفة يُعزل السجل برمز العزل المناسب.
- **Relevant File:** `src/quality_rules.py`
- **Relevant Function:** `validate_and_recalculate_order()`
- **Why We Used This Design:** Fixes corrupted order totals when underlying transactional items are valid.
- **Alternative:** Quarantining every single order total mismatch.
- **Why Not Used:** Unnecessarily isolates recoverable records where item-level details are intact.

---

## Question 19: How do you prevent Out-Of-Memory (OOM) errors in `src/elt_pipeline.py`?
- **Short English Answer:** We use memory-safe streaming buffers (`batch_size = 10,000`). Documents are processed from the cursor, accumulated in small buffers, flushed via `bulk_write`, and immediately cleared from RAM.
- **Arabic Explanation:** باستخدام الـ Streaming Buffers بحجم 10,000 وثيقة؛ حيث تُقرأ السجلات من المؤشر وتُعالج بالدفعات وتُكتب مباشرة إلى MongoDB عبر `bulk_write` مع تفريغ الـ Buffer فوراً من RAM.
- **Relevant File:** `src/elt_pipeline.py`
- **Relevant Function:** `run_elt_transform_and_classify()` & `flush_validated_batch()`
- **Why We Used This Design:** Guarantees $O(1)$ memory consumption for arbitrarily large datasets.
- **Alternative:** Storing all validated records in a single Python list before writing.
- **Why Not Used:** Causes memory exhaustion on millions of records.

---

## Question 20: What is the purpose of `src/incremental_loader.py`?
- **Short English Answer:** It is a documented extension module representing Optional Path B (Incremental Pipeline / CDC). The core submission natively achieves incremental and full idempotency via database-level upserts.
- **Arabic Explanation:** هو ملف توثيقي مخصص للمسار الاختياري Path B. المعمارية الأساسية للمشروع تنجز التحديثات التدريجية وكامل المتطلبات بكفاءة عبر الـ Idempotent Upsert.
- **Relevant File:** `src/incremental_loader.py`
- **Relevant Function:** `run_incremental_sync()`
- **Why We Used This Design:** Explicitly fulfills project structure requirements without adding bloat.
- **Alternative:** Coupling complex streaming CDC into the main batch flow.
- **Why Not Used:** Overcomplicates the individual core requirements.
