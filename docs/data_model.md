# Data Model — Olist E-Commerce Data Platform

## Architecture Medallion — Flow des données

```
SOURCES (CSV Kaggle)          BRONZE              SILVER              GOLD
─────────────────────         ──────              ──────              ────

olist_orders.csv          →   bronze.orders   →   silver.orders   ─┐
olist_order_items.csv     →   bronze.items    →   silver.items    ─┤
olist_payments.csv        →   bronze.payments →   silver.payments ─┼──→ fact_orders
olist_reviews.csv         →   bronze.reviews  →      (direct)     ─┘

olist_customers.csv       →   bronze.customers →  silver.customers → dim_customer (SCD2)
olist_products.csv        →   bronze.products  →  silver.products  → dim_product  (SCD2)
olist_sellers.csv         →   bronze.sellers   →  silver.sellers   → dim_seller   (SCD1)
olist_geolocation.csv     →   bronze.geo       →  silver.geo       → dim_geography(SCD1)
                                                                    → dim_date     (SCD0, générée)
```

---

## Star Schema — Gold Layer

```
                        dim_date (SCD0)
                        ─────────────
                        date_sk (PK)
                        date_id
                        year, month, quarter
                        day_of_week
                        is_weekend
                             │
                             │
dim_customer (SCD2)          │          dim_product (SCD2)
───────────────────          │          ──────────────────
customer_sk (PK)             │          product_sk (PK)
customer_id                  │          product_id
customer_unique_id    ┌──────┴──────┐   name (PT→EN)
city, state           │             │   category
zip_code              │ fact_orders │   unit_price
segment               │             │   weight_g
valid_from            │  order_sk   │   valid_from
valid_to              │  ──────── (PK)  valid_to
is_current ───────────│customer_sk  │   is_current ────────┐
                      │product_sk   │                       │
                      │seller_sk    │                       │
dim_seller (SCD1)     │date_sk      │   dim_geography(SCD1) │
──────────────────────│geo_sk       │   ─────────────────   │
seller_sk (PK)        │             │   geo_sk (PK)         │
seller_id             │ quantity    │   zip_code            │
city, state           │ price       │   city, state ────────┘
zip_code ─────────────│freight_value│   lat, lng
                      │payment_value│
                      │payment_inst.│
                      │review_score │
                      │order_status │
                      │delivery_days│
                      │delay_days   │
                      └─────────────┘
```

---

## Dimensions — Détail des colonnes

### dim_customer (SCD Type 2)

| Colonne | Type | Description |
|---|---|---|
| customer_sk | string (UUID) | Surrogate key (PK) |
| customer_id | string | Clé naturelle technique (par commande) |
| customer_unique_id | string | Vrai identifiant unique du client |
| city | string | Ville |
| state | string | État brésilien (2 lettres) |
| zip_code | string | Code postal |
| segment | string | Segment client (calculé en Silver) |
| valid_from | date | Début de validité de la ligne |
| valid_to | date | Fin de validité (9999-12-31 si courante) |
| is_current | boolean | True = ligne active |

**Colonnes trackées SCD2** : `city`, `state`, `segment`

---

### dim_product (SCD Type 2)

| Colonne | Type | Description |
|---|---|---|
| product_sk | string (UUID) | Surrogate key (PK) |
| product_id | string | Clé naturelle |
| name | string | Nom traduit PT → EN |
| category | string | Catégorie traduite PT → EN |
| unit_price | double | Prix unitaire |
| weight_g | integer | Poids en grammes |
| valid_from | date | Début de validité |
| valid_to | date | Fin de validité |
| is_current | boolean | True = ligne active |

**Colonnes trackées SCD2** : `category`, `unit_price`, `weight_g`

---

### dim_seller (SCD Type 1)

| Colonne | Type | Description |
|---|---|---|
| seller_sk | string (UUID) | Surrogate key (PK) |
| seller_id | string | Clé naturelle |
| city | string | Ville |
| state | string | État |
| zip_code | string | Code postal |

---

### dim_geography (SCD Type 1)

| Colonne | Type | Description |
|---|---|---|
| geo_sk | string (UUID) | Surrogate key (PK) |
| zip_code | string | Code postal (clé naturelle) |
| city | string | Ville |
| state | string | État |
| lat | double | Latitude (médiane par zip) |
| lng | double | Longitude (médiane par zip) |

---

### dim_date (SCD Type 0)

| Colonne | Type | Description |
|---|---|---|
| date_sk | string (UUID) | Surrogate key (PK) |
| date_id | date | Date (clé naturelle) |
| year | integer | Année |
| month | integer | Mois (1–12) |
| quarter | integer | Trimestre (1–4) |
| day_of_week | integer | Jour de la semaine (1=Lundi) |
| day_name | string | Nom du jour (Monday…) |
| month_name | string | Nom du mois (January…) |
| is_weekend | boolean | True si samedi ou dimanche |
| is_holiday_br | boolean | True si jour férié brésilien |

---

### fact_orders

| Colonne | Type | Description |
|---|---|---|
| order_sk | string (UUID) | Surrogate key (PK) |
| customer_sk | string | FK → dim_customer |
| product_sk | string | FK → dim_product |
| seller_sk | string | FK → dim_seller |
| date_sk | string | FK → dim_date (date d'achat) |
| geo_sk | string | FK → dim_geography |
| order_id | string | Clé naturelle |
| order_item_id | integer | Numéro d'item dans la commande |
| quantity | integer | Quantité |
| price | double | Prix unitaire au moment de la commande |
| freight_value | double | Frais de livraison |
| payment_value | double | Montant total payé |
| payment_installments | integer | Nombre de mensualités |
| payment_type | string | credit_card, boleto, voucher, debit_card |
| review_score | integer | Note client (1–5) |
| order_status | string | delivered, shipped, canceled… |
| delivery_days | integer | Jours entre achat et livraison effective |
| delay_days | integer | Retard vs date estimée (négatif = en avance) |
| order_year_month | string | Partition : format YYYY-MM |

---

## Règles SCD

```
SCD Type 0 — dim_date
  Générée une fois (2015–2030), jamais modifiée

SCD Type 1 — dim_seller, dim_geography
  Changement → OVERWRITE la ligne existante
  Pas d'historique conservé

SCD Type 2 — dim_customer, dim_product
  Changement → ferme l'ancienne ligne (valid_to = today, is_current = false)
             → insère une nouvelle ligne (valid_from = today, valid_to = 9999-12-31, is_current = true)
  Historique complet conservé
```

---

## Ce que chaque couche garantit

| Couche | Garantie |
|---|---|
| **Bronze** | Données brutes jamais modifiées. Colonnes techniques ajoutées (`_ingested_at`, `_source_file`, `_batch_id`). Lignes corrompues → table `_quarantine`. |
| **Silver** | Données nettoyées, dédupliquées, typées correctement, validées par les checks DQ. |
| **Gold** | Star schema prêt pour l'analyse. Surrogate keys UUID. `fact_orders` partitionnée par `order_year_month`. |

---

## Décisions techniques

| Décision | Choix | Raison |
|---|---|---|
| Surrogate keys | UUID (`uuid()` Spark) | Pas d'auto-increment en distribué |
| SCD2 change detection | Hash SHA-256 des colonnes trackées | Comparaison efficace en Spark |
| Fact table écriture | OVERWRITE sur partition `order_year_month` | Idempotent — rejouable sans doublons |
| Bronze | Append-only | Historique brut préservé à jamais |
| Silver | MERGE sur clé naturelle | Idempotent |
| Partitionnement fact | `order_year_month` | Pruning efficace pour les requêtes par période |
