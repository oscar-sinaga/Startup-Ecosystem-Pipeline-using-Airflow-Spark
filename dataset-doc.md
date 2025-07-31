# Dataset Description

## Database

ERD:
![Startup Public Image](https://sekolahdata-assets.s3.ap-southeast-1.amazonaws.com/notebook-images/mde-data-ingestion-spark/startup_-_public.png 'ERD')


### Company 
contains detailed information about various startup company locaation

| Column Name     | Description                        |
|-----------------|------------------------------------|
| `office_id`     | Unique identifier for the office.  |
| `object_id`     | Identifier for the associated object. |
| `description`   | Description of the office.         |
| `region`        | Region where the office is located.|
| `address1`      | Primary address line of the office.|
| `address2`      | Secondary address line of the office.|
| `city`          | City where the office is located.  |
| `zip_code`      | ZIP code of the office location.   |
| `state_code`    | State code of the office location. |
| `country_code`  | Country code of the office location. |
| `latitude`      | Latitude of the office location.   |
| `longitude`     | Longitude of the office location.  |
| `created_at`    | Timestamp when the record was created. |
| `updated_at`    | Timestamp when the record was last updated. |


### Funds
captures information about various funding rounds or funds associated with companies. Each record in this table represents a specific fund raised by a company, including details such as the fund amount, currency, and the date it was raised. 

| Column Name            | Description                                                |
|------------------------|------------------------------------------------------------|
| `fund_id`              | Unique identifier for the fund.                            |
| `object_id`            | Identifier for the associated company (foreign key).       |
| `"name"`               | Name of the fund.                                          |
| `funded_at`            | Date when the fund was raised.                             |
| `raised_amount`        | Amount of money raised by the fund.                        |
| `raised_currency_code` | Currency code of the raised amount (e.g., USD).            |
| `source_url`           | URL providing additional information about the fund.       |
| `source_description`   | Description or source of the information.                  |
| `created_at`           | Timestamp when the fund record was created.                |
| `updated_at`           | Timestamp when the fund record was last updated.           |

### Acquisition
record and manage details about company acquisitions, including which companies are involved and the financial aspects of the acquisition

| Column Name             | Description                                                    |
|-------------------------|----------------------------------------------------------------|
| `acquisition_id`        | Unique identifier for the acquisition.                         |
| `acquiring_object_id`   | Identifier for the acquiring company (foreign key).            |
| `acquired_object_id`    | Identifier for the acquired company (foreign key).             |
| `term_code`             | Term or type of acquisition.                                   |
| `price_amount`          | Amount paid for the acquisition.                               |
| `price_currency_code`   | Currency code of the acquisition price (e.g., USD).            |
| `acquired_at`           | Timestamp when the acquisition took place.                    |
| `source_url`            | URL providing additional information about the acquisition.   |
| `source_description`    | Description or source of the acquisition information.          |
| `created_at`            | Timestamp when the acquisition record was created.             |
| `updated_at`            | Timestamp when the acquisition record was last updated.        |


### Funding Rounds
information about various funding rounds for companies. Each entry represents a specific round of funding, including financial details such as the amount raised and valuations.

| Column Name                 | Description                                                    |
|-----------------------------|----------------------------------------------------------------|
| `funding_round_id`          | Unique identifier for the funding round. This serves as the primary key for the table. |
| `object_id`                 | Identifier for the company that received the funding. (foreign key)|
| `funded_at`                 | Date when the funding round occurred.                          |
| `funding_round_type`        | Type of funding round (e.g., Series A, Series B).        |
| `funding_round_code`        | Code or designation for the funding round (if applicable).     |
| `raised_amount_usd`         | Amount of money raised in USD.                                |
| `raised_amount`             | Amount of money raised in the specified currency.              |
| `raised_currency_code`      | Currency code for the raised amount (e.g., USD).              |
| `pre_money_valuation_usd`   | Pre-money valuation of the company in USD before the funding round. |
| `pre_money_valuation`       | Pre-money valuation of the company in the specified currency.  |
| `pre_money_currency_code`   | Currency code for the pre-money valuation.                    |
| `post_money_valuation_usd`  | Post-money valuation of the company in USD after the funding round. |
| `post_money_valuation`      | Post-money valuation of the company in the specified currency. |
| `post_money_currency_code`  | Currency code for the post-money valuation.                   |
| `participants`              | List of participants or investors in the funding round.        |
| `is_first_round`            | Boolean indicating if this is the company's first funding round. |
| `is_last_round`             | Boolean indicating if this is the company's last funding round. |
| `source_url`                | URL providing additional information about the funding round. |
| `source_description`        | Description or source of the funding round information.        |
| `created_by`                | Identifier of the user or system that created the record.      |
| `created_at`                | Timestamp when the funding round record was created.           |
| `updated_at`                | Timestamp when the funding round record was last updated.      |


### Investment
records details about investments made in various funding rounds. Each entry represents a specific investment, capturing the relationship between the investor, the company receiving the investment, and the funding round.

| Column Name         | Description                                                            |
|---------------------|------------------------------------------------------------------------|
| `investment_id`     | Unique identifier for the investment record. This serves as the primary key for the table. |
| `funding_round_id`  | Identifier for the funding round in which the investment was made. (foreign key) |
| `funded_object_id`  | Identifier for the company or entity that received the investment.  (foreign key)|
| `investor_object_id` | Identifier for the investor or entity that made the investment.  (foreign key) |
| `created_at`        | Timestamp when the investment record was created.                      |
| `updated_at`        | Timestamp when the investment record was last updated.                 |

### IPOS
details about initial public offerings (IPOs) of companies. Each record represents an IPO event, capturing key information such as the valuation and raised amounts, the date the company went public, and the stock symbol under which the company is traded.

| Column Name           | Description                                                                |
|-----------------------|----------------------------------------------------------------------------|
| `ipo_id`              | Unique identifier for the IPO record. This serves as the primary key for the table. |
| `object_id`           | Identifier for the company that went public. This is a foreign key referencing the `company` table. |
| `valuation_amount`    | The valuation amount of the company during the IPO, in the specified currency. |
| `valuation_currency_code` | Currency code for the valuation amount (e.g., USD).                       |
| `raised_amount`       | The amount of money raised during the IPO, in the specified currency.      |
| `raised_currency_code` | Currency code for the raised amount (e.g., USD).                           |
| `public_at`           | Timestamp indicating when the company went public.                         |
| `stock_symbol`        | Stock symbol for the company on the stock exchange (e.g., NASDAQ:AAPL).     |
| `source_url`          | URL of the source where IPO information was obtained.                       |
| `source_description`  | Description of the source where IPO information was obtained.               |
| `created_at`          | Timestamp when the IPO record was created.                                  |
| `updated_at`          | Timestamp when the IPO record was last updated.                             |

## File

### People
contains information about individuals associated with entities in the startup investment context.

| Column Name     | Description                                                   |
|-----------------|---------------------------------------------------------------|
| `people_id`     | Unique identifier for the person. This serves as the primary key for the table. |
| `object_id`     | Identifier for the company entity the person is associated with. |
| `first_name`    | First name of the person.                                    |
| `last_name`     | Last name of the person.                                     |
| `birthplace`    | Place where the person was born.                             |
| `affiliation_name` | Name of the organization or company with which the person is affiliated. |

### Relationship

tracks various relationships between individuals and companies within the startup investment ecosystem. 

| Column Name            | Description                                                       |
|------------------------|-------------------------------------------------------------------|
| `relationship_id`      | Unique identifier for the relationship record. This serves as the primary key for the table. |
| `person_object_id`     | Identifier for the person involved in the relationship. |
| `relationship_object_id` | Identifier for the entity (company) involved in the relationship |
| `start_at`             | Date when the relationship started.                              |
| `end_at`               | Date when the relationship ended, if applicable.                  |
| `is_past`              | Boolean indicating whether the relationship is in the past (`true`) or ongoing (`false`). |
| `sequence`             | Sequence number indicating the order or importance of the relationship. |
| `title`                | Title or role of the person in the relationship (e.g., Co-Founder, CEO). |
| `created_at`           | Timestamp when the record was created.                            |
| `updated_at`           | Timestamp when the record was last updated.                       |

## API

### Milestones
Access: https://api-milestones.vercel.app/api/data?start_date=date&end_date=date
Example: https://api-milestones.vercel.app/api/data?start_date=2008-06-09&end_date=2008-06-29

To retrieve milestones within a specific date range, use the API endpoint and provide `start_date` and `end_date` parameters in the URL. The API will return milestone records including creation and update timestamps, description, milestone date, and related URLs for further details.

| Column Name       | Description                                                                                      |
|-------------------|--------------------------------------------------------------------------------------------------|
| `created_at`      | Timestamp when the milestone record was created.                                                 |
| `description`     | A brief description of the milestone event.                                                        |
| `milestone_at`    | Date when the milestone occurred.                                                                  |
| `milestone_code`  | Code representing the type or category of the milestone.                                          |
| `milestone_id`    | Unique identifier for the milestone record.                                                       |
| `object_id`       | Identifier for the related company or entity (references `object_id` in the `company` table).     |
| `source_description` | Description of the source or context for the milestone.                                           |
| `source_url`      | URL to the source or article related to the milestone.                                              |
| `updated_at`      | Timestamp when the milestone record was last updated.                                              |
