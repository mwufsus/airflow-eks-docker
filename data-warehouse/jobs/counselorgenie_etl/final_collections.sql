create table if not exists dimensions_counselorgenie.f_credit_collections (
  collections_id varchar(64) PRIMARY KEY,
  originalLenderName varchar(128),
  accountNumber varchar(128),
  currentBalance int,
  pastDue int,
  originalBalance int,
  remarkCode varchar(64),
  subscriberMemberCode varchar(64),
  dateOpened varchar(64),
  portfolioType varchar(64),
  ecoaDesignator varchar(64),
  subscriberIndustryCode varchar(64),
  subscriberName varchar(64),
  creditReportId varchar(64),
  accountRating varchar(64),
  environmentName varchar(128),
  updatedAt timestamp,
  createdAt timestamp,
  record_created_at timestamp
);

TRUNCATE TABLE dimensions_counselorgenie.f_credit_collections;

SET
  enable_case_sensitive_identifier to TRUE;

INSERT INTO
  dimensions_counselorgenie.f_credit_collections
SELECT
  rdata."id" :: varchar(64) as collections_id,
  rdata."originalLenderName" :: varchar(128) as originalLenderName,
  rdata."accountNumber" :: varchar(128) as accountNumber,
  rdata."currentBalance" :: int as currentBalance,
  rdata."pastDue" :: int as pastDue,
  rdata."originalBalance" :: int as originalBalance,
  rdata."remarkCode" :: varchar(64) as remarkCode,
  rdata."subscriberMemberCode" :: varchar(64) as subscriberMemberCode,
  rdata."dateOpened" :: varchar(64) as dateOpened,
  rdata."portfolioType" :: varchar(64) as portfolioType,
  rdata."ecoaDesignator" :: varchar(64) as ecoaDesignator,
  rdata."subscriberIndustryCode" :: varchar(64) as subscriberIndustryCode,
  rdata."subscriberName" :: varchar(64) as subscriberName,
  rdata."creditReportId" :: varchar(64) as creditReportId,
  rdata."accountRating" :: varchar(64) as accountNumber,
  rdata."environmentName" :: varchar(128) as environmentName,
  rdata."updatedAt" :: timestamp as updatedAt,
  rdata."createdAt" :: timestamp as createdAt,
  getdate() as record_created_at
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY rdata."id" :: varchar(64)
        ORDER BY
          rdata."createdAt" :: timestamp DESC
      ) AS rn
    FROM
      staging_counselorgenie.staging_collections
  )
WHERE
  rn = 1
  AND rdata."environmentName" :: varchar(128) not in ('master', 'main', 'staging', 'prod');