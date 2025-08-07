# Proyek Data Pipeline Using Pyspark and Airflow: Startup Ecosystem

Sebuah data pipeline end-to-end untuk mengintegrasikan, memproses, dan menganalisis data ekosistem startup dari berbagai sumber. Proyek ini dibuat memungkinkan analisis mendalam terhadap tren investasi, kinerja perusahaan, dan jaringan para pemain kunci. Proyek ini merupakan lanjutan dari proyek [Data Pipeline with Pyspark](https://github.com/oscar-sinaga/data_pipeline_pyspark) dengan modifikasi orkestrasi menggunakan **Apache Airflow**.

## Daftar Isi
- [Proyek Data Pipeline Using Pyspark and Airflow: Startup Ecosystem](#proyek-data-pipeline-using-pyspark-and-airflow-startup-ecosystem)
  - [Daftar Isi](#daftar-isi)
  - [Requirements Gathering \& Solution](#requirements-gathering--solution)
    - [Latar Belakang Masalah (Background Problem)](#latar-belakang-masalah-background-problem)
      - [1. Ketidakmampuan Mengevaluasi Momentum Pertumbuhan Secara Akurat](#1-ketidakmampuan-mengevaluasi-momentum-pertumbuhan-secara-akurat)
      - [2. Analisis Strategi *Exit* yang Terfragmentasi](#2-analisis-strategi-exit-yang-terfragmentasi)
      - [3. Keterbatasan dalam Pemetaan Jaringan Modal Manusia](#3-keterbatasan-dalam-pemetaan-jaringan-modal-manusia)
    - [Solusi yang Diusulkan (Proposed Solution)](#solusi-yang-diusulkan-proposed-solution)
    - [Profiling Data](#profiling-data)
    - [Desain Arsitektur Pipeline](#desain-arsitektur-pipeline)
  - [Desain Target Database (Data Warehouse)](#desain-target-database-data-warehouse)
    - [Proses Bisnis 1: Evaluasi Perjalanan Pendanaan dan Pertumbuhan Startup](#proses-bisnis-1-evaluasi-perjalanan-pendanaan-dan-pertumbuhan-startup)
      - [Tabel Fakta:](#tabel-fakta)
    - [Proses Bisnis 2: Analisis Strategi Exit dan Kinerja Pasar Startup](#proses-bisnis-2-analisis-strategi-exit-dan-kinerja-pasar-startup)
      - [Tabel Fakta](#tabel-fakta-1)
        - [**`fact_acquisition`**](#fact_acquisition)
        - [**`fact_ipos`**](#fact_ipos)
      - [Tabel Dimensi](#tabel-dimensi)
        - [**`dim_company`**](#dim_company)
        - [**`dim_date`**](#dim_date)
    - [Proses Bisnis 3: Pemetaan Ekosistem dan Jaringan Penggerak Startup](#proses-bisnis-3-pemetaan-ekosistem-dan-jaringan-penggerak-startup)
      - [Tabel Fakta:](#tabel-fakta-2)
      - [Tabel Dimensi:](#tabel-dimensi-1)
    - [Ringkasan Final Desain Data Warehouse](#ringkasan-final-desain-data-warehouse)
      - [Tabel Dimensi – Memberikan Konteks *Siapa, Apa, Di Mana, Kapan*](#tabel-dimensi--memberikan-konteks-siapa-apa-di-mana-kapan)
      - [Tabel Fakta – Perekam Peristiwa \& Ukuran Bisnis](#tabel-fakta--perekam-peristiwa--ukuran-bisnis)
  - [Desain Alur Kerja ETL](#desain-alur-kerja-etl)
    - [Staging Layer](#staging-layer)
    - [Warehouse Layer](#warehouse-layer)
  - [Teknologi yang Digunakan](#teknologi-yang-digunakan)
  - [Cara Menjalankan Pipeline](#cara-menjalankan-pipeline)


## Requirements Gathering & Solution

### Latar Belakang Masalah (Background Problem)
Perusahaan **"VenturePulse"** adalah perusahaan konsultan investasi yang mempunyai klien dari berbagai perusahaan startup hingga institusi keuangan. Dalam menjalankan misinya, VenturePulse menghadapi kendala utama dalam mengintegrasikan dan menganalisis informasi dan data dari berbagai sumber secara menyeluruh. Informasi dan penjelasan mengenai berbagai data tersebut bisa dilihat di [dataset-doc.md](dataset-doc.md). Keterbatasan dan kerumitan akses terhadap data yang tersebar di berbagai format dan sumber tersebut menyebabkan beberapa masalah bisnis sebagai berikut:

#### 1. Ketidakmampuan Mengevaluasi Momentum Pertumbuhan Secara Akurat

- **Kondisi:** Data pendanaan (`funding_rounds`, `investments`, `funds`) dan pencapaian (`milestones`) tersedia dari berbagai sumber, namun belum dihubungkan secara eksplisit dalam model analitik.
- **Masalah:** Sulit untuk menilai dampak dari pendanaan terhadap pertumbuhan startup secara langsung. Pertanyaan seperti “apakah pendanaan Seri B mendorong peluncuran produk utama?” tidak dapat dijawab secara langsung karena tidak adanya keterkaitan yang jelas antara waktu, sumber dana, dan pencapaian bisnis.

**Tabel kunci:**
- `funding_rounds`, `investments`, `funds`, `milestones`

---

#### 2. Analisis Strategi *Exit* yang Terfragmentasi

- **Kondisi:** Data akuisisi (`acquisitions`) dan IPO (`ipos`) tersedia dalam database, namun belum dilengkapi dengan atribut deskriptif perusahaan atau waktu yang mendetail untuk mendukung analisis longitudinal dan sektoral.
- **Masalah:** Tanpa model data yang terstruktur untuk membandingkan aktivitas dan nilai exit, sulit melakukan analisis perbandingan antar industri, waktu, atau jenis strategi exit. Pertanyaan seperti “berapa rata-rata valuasi IPO di sektor fintech dalam 5 tahun terakhir” atau “korporasi mana yang paling sering mengakuisisi startup” tidak bisa dijawab secara efisien.

**Tabel kunci:**
- `acquisitions`, `ipos`,  `ipos`

---

#### 3. Keterbatasan dalam Pemetaan Jaringan Modal Manusia

- **Kondisi:** Data `relationships` yang menghubungkan individu ke perusahaan tersedia, dan pencapaian perusahaan (`milestones`) juga tersedia, namun belum terintegrasi dalam satu sumber terpusat agar pelacakan karier dan inovasi mudah dan dapat dihubungkan dengan berbagai data lain.
- **Masalah:** Sulit melacak jejak kontribusi individu terhadap pertumbuhan dan inovasi lintas perusahaan. Visualisasi jaringan atau analisis dampak modal manusia terhadap performa startup tidak dapat dilakukan secara utuh karena keterbatasan keterkaitan antara individu, peran, dan hasil nyata yang dicapai.

**Tabel kunci:**
- `relationships`, `milestones`, `people`, `company`


### Solusi yang Diusulkan (Proposed Solution)

Oleh karena itu perlu dibangun **Data Pipeline Terpusat** yang mengotomatisasi proses pengumpulan, pembersihan, transformasi, dan penyimpanan data dari berbagai sumber ke dalam sebuah **Data Warehouse** tunggal.

Tujuannya adalah menyediakan data yang andal, terintegrasi, dan siap pakai untuk analisis strategis tanpa intervensi manual berlebihan.

### Profiling Data

Proses profiling data membantu mengidentifikasi masalah kualitas data yang spesifik. Temuan ini menjadi dasar penting dalam menentukan langkah-langkah transformasi pada proses ETL, karena langsung memengaruhi keakuratan dan kelengkapan analisis bisnis yang akan dilakukan.

**1. Kelengkapan Data (Completeness) yang Rendah pada Metrik Kunci**

Banyak kolom krusial untuk analisis bisnis memiliki persentase *missing values* yang sangat tinggi. Hal ini secara langsung menghambat proses bisnis yang telah didefinisikan:

* **Dampak pada Analisis Pendanaan & Exit:**
    * Pada tabel `funding_rounds`, data valuasi sangat tidak lengkap, dengan **48.1%** nilai hilang pada `pre_money_currency_code` dan **41.63%** pada `post_money_currency_code`.
    * Pada tabel `acquisitions`, **81.67%** data `term_code` (tipe akuisisi: cash/stock) hilang.
    * **Implikasi:** Tanpa data ini `pre_money_currency_code` dan`post_money_currency_code` analisis yang terkait dengan amount **mustahil** untuk untuk mengevaluasi kinerja pendanaan atau menganalisis strategi *exit* secara komprehensif jika tiap amount bergantung sama uangnya. Proses ETL harus menerapkan strategi untuk menangani nilai-nilai yang hilang ini sebelum memuatnya ke `fact_investment_round_participation` dan `fact_acquisitions`. 
      * Pada tabel `funding_rounds`, kolom  `post_money_currency_code` dan `pre_money_currency_code` akan kita drop karena kolom `pre_money_valuation_usd` dan kolom `post_money_valuation_usd` datanya tidak kosong dan sudah dalam usd sehingga tidak perlukan lagi kedua kolom mata tersebut.
      * Pada tabel `acquisitions` kolom `term_code` akan diisi `Unknown` untuk data yang hilang

* **Dampak pada Pemetaan Jaringan & Karier:**
    * Tabel `relationship` memiliki data tanggal yang sangat minim: `start_at` hilang **53.48%** dan `end_at` hilang **85.71%**.
    * **Implikasi:** Ini secara fundamental merusak kemampuan untuk "Memetakan Jaringan Modal Manusia". Analisis durasi karier atau periode aktif seseorang di sebuah perusahaan menjadi tidak akurat. Transformasi pada `fact_relationship` harus mampu menangani tanggal yang kosong ini.
      * Oleh karena itu kita akan mengisi kedua kolom ini dengan tanggal jauh di masa depan (2100-01-01) untuk menandakan tanggal ini kosong. Sehingga bisa difilter saat analisis nantinya.

* **Dampak pada Analisis Geografis:**
    * Informasi lokasi pada tabel `company` juga tidak lengkap, seperti `state_code` (**42.36%** hilang) dan `city` (**4.59%** hilang).
    * **Implikasi:** Analisis berbasis lokasi menjadi kurang andal.

**2. Integritas dan Format Data yang Belum Standar**

* **Tipe Data Tanggal:** Sebagian besar kolom tanggal di berbagai tabel (`funding_rounds.funded_at`, `acquisitions.acquired_at`, `relationship.start_at`) ditemukan sebagai tipe data `object` (string), bukan `datetime`.
* **Implikasi:** Ini menyebabkan semua bentuk analisis berbasis waktu (tren, durasi, dsb.) terhambat. Oleh karena itu semua kolom tanggal harus dikonversi ke format `YYYYMMDD` sebagai *foreign key* ke `date_id` dari tabel `dim_date`.

### Desain Arsitektur Pipeline

Pipeline terdiri dari beberapa komponen utama:

- **Layers:**
  - **Data Sources:** PostgreSQL, file CSV/JSON, dan API eksternal.
  - **Staging Layer:** Menyimpan data mentah di PostgreSQL schema `staging` untuk proses transformasi lebih lanjut.
  - **Warehouse Layer:** Menyimpan data terstruktur yang sudah ditransformasi dalam schema `warehouse` berbasis Star Schema.

- **Logging & Monitoring:**
  - Informasi setiap proses ETL akan dicatat dalam database log **PosgreSQL**.

- **Orchestration and Schedulling**
  - Semua Proses ETL dijalankan dan dijadwalkan oleh sebuah tools orkestrasi **Apache Airflow**   

- **Temporary Data and Validation**
  - Setiap temporary data antara proses extract dan load disimpan dalam bucket-bucket di data lake **Minio** dalam bentuk **csv** dan **parquet**.
  - Data invalid juga akan disimpan dalam data lake **Minio**.

- **Error Alert**
  - Setiap error dalam proses dag akan dikirimkan ke **Slack** 

![Pipeline Design](picture/data_pipeline_workflow.drawio.png)

## Desain Target Database (Data Warehouse)

Struktur data warehouse dirancang berdasarkan prinsip **Kimball** menggunakan pendekatan **Star Schema**. Model ini menjadikan **tabel fakta** sebagai pusat penyimpanan peristiwa bisnis yang terukur, dikelilingi oleh **tabel dimensi** yang memberikan konteks ("siapa", "apa", "kapan", "di mana").

---

### Proses Bisnis 1: Evaluasi Perjalanan Pendanaan dan Pertumbuhan Startup

Fokus: Mengukur aliran modal, momentum pertumbuhan, dan jaringan pendanaan startup.

#### Tabel Fakta:
- **`fact_investment_round_participation`**
  - **Grain:**  Satu baris mewakili satu keterlibatan unik investor dalam satu putaran pendanaan untuk sebuah perusahaan, termasuk informasi nilai investasi (jika ada), posisi dalam ronde, dan tahap investasi.
  - **Peran:** Tabel paling krusial untuk analisis pendanaan. Tabel ini memungkinkan analisis jaringan co-investor dan menjawab pertanyaan seperti "siapa berinvestasi bersama siapa?"

- **`fact_funds`**
  
  - **Grain:** Satu baris merepresentasikan satu fund (dana kelolaan) yang berhasil dikumpulkan oleh investor pada waktu tertentu.
  - **Peran:** Tabel ini mencatat detail fund (dana kelolaan) dari investor (misal VC/PE) yang akan digunakan untuk berinvestasi dalam berbagai startup. Tabel ini berguna untuk:
    - Menganalisis kapasitas pendanaan suatu investor (misalnya: total fund yang pernah dikumpulkan),
    - Melacak pertumbuhan dan sejarah aktivitas pengumpulan dana

- **`fact_milestones`**
  
  - **Grain:** 
    Satu baris merepresentasikan satu peristiwa milestone unik (misalnya peluncuran produk, akuisisi, kerja sama, fundraising) yang terjadi pada satu perusahaan, pada satu tanggal tertentu.

  - **Peran:**
    Menyediakan informasi naratif dan temporal tentang pencapaian atau aktivitas penting perusahaan yang tidak tercermin langsung dalam angka pendanaan formal.

- **`dim_company`**
  - **Grain:**
    Satu baris merepresentasikan satu entitas perusahaan unik.

  - **Peran:**
    Menyediakan informasi deskriptif dan geografis perusahaan yang mendukung analisis lintas domain.


- **`dim_date`**
  - **Grain:** Satu baris per hari kalender.
  - **Peran:** memberikan kerangka waktu untuk seluruh analisis tren pendanaan.

---

### Proses Bisnis 2: Analisis Strategi Exit dan Kinerja Pasar Startup

**Fokus:**  
Menganalisis peristiwa puncak seperti akuisisi dan IPO dalam siklus hidup startup, serta mengevaluasi nilai dan waktu exit sebagai bagian dari strategi investasi.


### Tabel Fakta

#### **`fact_acquisition`**
* **Grain:**  
  Satu baris mewakili satu peristiwa akuisisi unik yang menghubungkan perusahaan pengakuisisi, perusahaan yang diakuisisi dan tanggal akuisisi.

* **Peran:**
  - Merekam volume dan nilai akuisisi sebagai indikator likuiditas pasar startup.
  - Menyediakan dasar analisis untuk tren akuisisi lintas waktu, wilayah, industri, atau ukuran valuasi.


#### **`fact_ipos`**
* **Grain:**  
  Satu baris mewakili satu peristiwa IPO unik, yang terkait ke perusahaan yang IPO dan tanggal IPO.

* **Peran:**
  - Mengukur jalur exit melalui pasar publik.
  - Menyediakan data valuasi dan dana yang dihimpun saat IPO.
  - Memungkinkan perbandingan antara strategi IPO vs akuisisi untuk startup.
  - Dapat digunakan untuk mengukur return investor jika dikombinasikan dengan data putaran pendanaan sebelumnya.


### Tabel Dimensi

#### **`dim_company`**
* **Grain:**  
  Satu baris mewakili satu entitas perusahaan unik.

* **Peran:**
  - Menjadi referensi deskriptif bagi perusahaan yang:
    - Melakukan akuisisi
    - Diakuisisi
    - Melakukan IPO
  - Menyediakan informasi lokasi, region, dan metadata geografis untuk analisis spasial.
  - Memungkinkan segmentasi berdasarkan wilayah, negara, atau kota.


#### **`dim_date`**
* **Grain:**  
  Satu baris mewakili satu hari kalender unik.

* **Peran:**
  - Memberikan kerangka waktu untuk menganalisis tren exit startup dari waktu ke waktu.
  - Memungkinkan agregasi berdasarkan minggu, bulan, kuartal, atau tahun.
  - Mendukung visualisasi tren musiman terhadap aktivitas akuisisi dan IPO.


### Proses Bisnis 3: Pemetaan Ekosistem dan Jaringan Penggerak Startup

Fokus: Memetakan kontribusi individu dan relasi dalam pertumbuhan ekosistem startup.

#### Tabel Fakta:
- **`fact_relationsip`**
  - **Grain:** Satu baris per hubungan kerja spesifik antara seorang individu dan perusahaan.
  - **Peran:** Sumber utama pemetaan modal manusia, sangat penting untuk pelacakan karier dan analisis jaringan profesional.

- **`fact_milestones`**
  - **Grain:** Satu baris merepresentasikan satu peristiwa atau pencapaian (milestone) spesifik yang terkait dengan sebuah perusahaan.
  - **Peran:** Memetakan hasil kerja dan bukti inovasi individu/perusahaan. Dapat digunakan untuk mengidentifikasi wilayah dengan frekuensi inovasi tinggi.

#### Tabel Dimensi:
- **`dim_people`**
  - **Peran:** Subjek utama dari pemetaan individu dan karier.
  - **Grain:** Satu baris per individu unik.

- **`dim_company`**
  - **Peran:** memberikan konteks lokasi organisasi atau perusahaan dan hubungan profesional.
  - **Grain:** Satu baris per perusahaan.

- **`dim_date`**
  - **Peran:** Menyediakan kerangka temporal untuk aktivitas profesional.
  - **Grain:** Satu baris per hari kalender.

---

### Ringkasan Final Desain Data Warehouse
Struktur mengikuti **Kimball Method** dengan **Star Schema**, di mana tabel **dimensi** menyimpan konteks (*who, what, where, when*) dan tabel **fakta** menyimpan peristiwa atau ukuran bisnis (*measurable events*).

---

#### Tabel Dimensi – Memberikan Konteks *Siapa, Apa, Di Mana, Kapan*
| Nama Tabel     | Peran                                                                                                                                          | Grain                                   |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| `dim_date`     | Kalender standar yang mengakomodasi analisis tren dan agregasi waktu (harian, mingguan, bulanan, kuartalan, tahunan, musiman)                  | Satu baris per hari kalender unik       |
| `dim_company`  | Representasi unik setiap perusahaan, mencakup informasi deskriptif (nama, industri) dan geografis (negara, kota, region)                       | Satu baris per perusahaan unik          |
| `dim_people`   | Representasi unik setiap individu dalam ekosistem startup (founder, eksekutif, investor, karyawan kunci) dengan atribut demografi & karier     | Satu baris per individu unik            |

---

#### Tabel Fakta – Perekam Peristiwa & Ukuran Bisnis
| Nama Tabel                              | Peran                                                                                                                                                                                                                 | Grain                                                                 |
|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| `fact_investment_round_participation`   | Merekam partisipasi unik investor dalam satu putaran pendanaan perusahaan, termasuk jumlah investasi, posisi dalam ronde, dan tahap pendanaan                                                                        | Satu baris per kombinasi investor–putaran pendanaan–perusahaan        |
| `fact_funds`                            | Merekam dana kelolaan (funds) yang dikumpulkan investor (VC, PE) pada waktu tertentu, digunakan untuk mengukur kapasitas dan sejarah pendanaan                                                                       | Satu baris per fund unik                                               |
| `fact_acquisitions`                     | Merekam peristiwa akuisisi startup (nilai, tanggal, pihak pengakuisisi dan yang diakuisisi), sebagai indikator likuiditas pasar dan strategi exit                                                                    | Satu baris per peristiwa akuisisi unik                                 |
| `fact_ipos`                             | Merekam peristiwa IPO (nilai saham, dana yang dihimpun, tanggal), digunakan untuk mengukur jalur exit dan perbandingan strategi IPO vs akuisisi                                                                      | Satu baris per peristiwa IPO unik                                      |
| `fact_milestones`                       | **Factless Fact Table** – Merekam pencapaian atau aktivitas penting perusahaan (misal peluncuran produk, kerja sama, fundraising) sebagai indikator inovasi & perkembangan                                           | Satu baris per milestone unik                                          |
| `fact_relationship`                     | **Factless Fact Table** – Merekam hubungan kerja unik antara individu dan perusahaan, untuk pemetaan modal manusia dan analisis jaringan profesional                                                                 | Satu baris per hubungan kerja unik                                     |


Pemetaan source-to-target secara detail disediakan dalam dokumen berikut : [source_to_target_mapping](source_to_target_mapping.md).

---

## Desain Alur Kerja ETL

### Staging Layer
1. **Extract:** PySpark menarik data dari berbagai sumber (API, database source dan spreadsheet). Kemudian data tersebut akan disimpan sementara di Bucket `extracted-data` di **Minio**
2. **Load:** Data yang sudah ditarik dari berbagai sumber oleh Pyspark ke **Minio** kemudian akan disimpan ke **Database Staging** PostgreSQL.  

### Warehouse Layer
1. **Extract dan Transform:** Data raw yang sudah disimpan di **Database Staging** kemudian diextract dan ditransform oleh pyspark. Proses Transformasi mencakup:
   - Pembersihan data (null, duplikat)
   - Standarisasi format
   - Enrichment kolom
   - Integrasi tabel berdasarkan `object_id`
  
   Setelah ditransformasi, data tersebut akan disimpan sementara di Bucket `transformed-data` di **Minio**

2. **Validation:** Data hasil transformasi kemudian divalidasi dan report hasil validasinya disimpan pada **Data Storage Minio** dalam bentuk *json*.
  
3. **Load:** 
Setelah divalidasi, data yang disimpan sementara di **Minio** kemudian di-load ke **Database Warehoyse** PostgreSQL :

Setiap proses ETL pada setiap tabel, informasi lognya akan disimpan pada **Database Log**. Alur ETL setiap layer ini diorkestrasi dengan **Airflow**. Jika ada Error yang terjadi pada suatu proses maka informasi errornya akan dikirim ke akun **Slack**. Data-Data akhir yang sudah ditransformasi dan disimpan database warehouse kemudian dapat divisualisasi dalam bentuk dashboard di **Metabase**. 

## Teknologi yang Digunakan

- **Bahasa:** Python
- **Engine Pemrosesan:** Apache Spark (via PySpark)
- **Orcrhestation:** Apache Airflow
- **Penyimpanan:**
  - PostgreSQL sebagai tools **database** (Staging & Warehouse)
  - Minio sebagai tools **data lake** (data Sementara hasil extract dan data hasil validasi)
- **Containerization:** Docker Image, Docker Compose
- **Visualization and Dashboard** : Metabase
- **Alerting** : Slack

## Cara Menjalankan Pipeline

1. **Prasyarat:**
   - Docker & Docker Compose

2. **Clone Repo:**
   ```bash
   git clone https://github.com/oscar-sinaga/Startup-Ecosystem-Pipeline-using-Airflow-Spark.git
   cd Startup-Ecosystem-Pipeline-using-Airflow-Spark

3. **Create env.file di project repo:**
   
   *Contoh :*

   ```bash
    AIRFLOW_UID=50000
    AIRFLOW_FERNET_KEY='rBEckHti5AkaGaAxQOMiXsC1FuZoufNy0I5xdBRP684='
    AIRFLOW_DB_URI=postgresql+psycopg2://airflow:airflow@airflow_metadata/airflow
    AIRFLOW_DB_USER=airflow
    AIRFLOW_DB_PASSWORD=airflow
    AIRFLOW_DB_NAME=airflow
    AIRFLOW_PORT=5433

    STARTUP_INVESTMENTS_DB_USER=postgres
    STARTUP_INVESTMENTS_DB_PASSWORD=postgres
    STARTUP_INVESTMENTS_DB_NAME=startup_investments_db
    STARTUP_INVESTMENTS_PORT=5434

    STAGING_DB_USER=postgres
    STAGING_DB_PASSWORD=postgres
    STAGING_DB_NAME=staging_db
    STAGING_PORT=5435

    WAREHOUSE_DB_USER=postgres
    WAREHOUSE_DB_PASSWORD=postgres
    WAREHOUSE_DB_NAME=warehouse_db
    WAREHOUSE_PORT=5437

    LOG_DB_USER=postgres
    LOG_DB_PASSWORD=postgres
    LOG_DB_NAME=log_db
    LOG_PORT=5436

    MINIO_ROOT_USER=minio
    MINIO_ROOT_PASSWORD=minio123

    MB_DB_TYPE=postgres
    MB_DB_DBNAME=metabase_db
    MB_DB_PORT=5432  # Ensure this matches PostgreSQL service
    MB_DB_USER=metabase_user
    MB_DB_PASS=metabase_pass
    MB_DB_HOST=metabase-db  # This should match the service name in docker-compose

    MB_ADMIN_EMAIL=admin@example.com
    MB_ADMIN_PASSWORD=admin123456
    ```

4. **Create Spredsheet Credentials**

    Terlebih dahulu buat credentials untuk akses ke spreadsheetnya. Disini file `data\source\people.csv` dan `data\source\relationships.csv` perlu di upload ke gdrive dijadikan spreadsheet  dan akses ke api spreadsheetnya akan diambil berdasarkan credentials yang digenerate oleh **Google Data Cloud Servive**.

5. **Save the credentials**
  
     Simpan credentials di folder `dags/include`

6.  **Membangun dan Menjalankan Services**:
    
    Jalankan perintah berikut untuk membangun image dan menjalankan semua service (Spark, PostgreSQL, Minio, Airflow) di background:
    ```bash
    docker-compose up -d --build
    ```
7. **Tambahkan Connections dan Variables**
   Tambahkan konfigurasi seperti `Connections` dan `Variables` agar proses di Airflow bisa terhubung dengan beberapa source seperti **Database PostgreSQL** ,  **Data Lake Minio** atau dengan alat preprocessing seperti **Apache Airflow** :
   ```bash
   docker exec -it airflow_standalone bash
   cd dags/include
   airflow connections import connections.yaml
   airflow variables import variables.json
   exit
    ```

8.  **Memicu ETL Job**:
    
    Untuk menjalankan pipeline, 
    - Buka endpoint **Airflow webserver** --> `localhost:8082` (sesuaikan dengan port airflow webserver)
    - Login ke airflow menggunakan username dan password yang diambil dari log container **airflow**: 
      ![log_airflow_standalone](picture/log_airflow_standalone.png)
    - kemudian jalankan salah satu dag seperti berikut:
      ![how_run_dag](picture/how_run_dag.png)

9. **Membuat Dashboard**
    Untuk membuat dashboard atau visualisasi
    - Buka endpoint **Metabase** --> `localhost:8003` (sesuaikan dengan port metabase)
    - Isi username/email dan password (sesuai dengan yang sudah dikonfigurasi di `.env`)
    - Mulai membuat visualisasi dan dashboard. Contoh dashboard:
  ![contoh_dashboard_metabase](picture/contoh_dashboard_metabase.png) 

