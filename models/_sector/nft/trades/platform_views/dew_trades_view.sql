{{ config(
        schema = 'dew',
        alias ='trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "dew",
                                    \'["shogun"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "dew"