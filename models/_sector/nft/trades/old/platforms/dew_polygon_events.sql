{{ config(
    schema = 'dew_polygon',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}
{% set c_native_token_address = "0x0000000000000000000000000000000000000000" %}
{% set c_alternative_token_address = "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270" %}  -- ETH
{% set c_native_symbol = "MATIC" %}
{% set min_block_number = 40705277 %}
{% set project_start_date = '2023-03-04' %}

with source_polygon_transactions as (
    select *
    from {{ source('polygon','transactions') }}
    {% if not is_incremental() %}
    where block_number >= {{min_block_number}}  -- dew first txn
    {% endif %}
    {% if is_incremental() %}
    where block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,ref_tokens_nft as (
    select *
    from {{ ref('tokens_nft') }}
    where blockchain = 'polygon'
)
,ref_tokens_erc20 as (
    select *
    from {{ ref('tokens_erc20') }}
    where blockchain = 'polygon'
)
,ref_nft_aggregators as (
    select *
    from {{ ref('nft_aggregators') }}
    where blockchain = 'polygon'
)
,source_prices_usd as (
    select *
    from {{ source('prices', 'usd') }}
    where blockchain = 'polygon'
    {% if not is_incremental() %}
      and minute >= '{{project_start_date}}'  -- first txn
    {% endif %}
    {% if is_incremental() %}
      and minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
,trades_data as (

    select
      erc20.evt_tx_hash as tx_hash
      ,COUNT(*) as no_of_items
    from {{ source('dew_polygon', 'dew_market_evt_Fulfilled') }} as dew
    {% if not is_incremental() %}
    where evt_block_time >= '{{project_start_date}}'  -- dew first txn
    {% endif %}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    GROUP BY 1
)
,events_raw as (
    select
       evt_block_time as block_time
       ,evt_block_number as block_number
       ,evt_tx_hash as tx_hash
       ,evt_index
       ,recipient as buyer
       ,maker as seller
       ,contract_address as project_contract_address
       ,bytea2numeric_v3(substring(data, 27 + 64 * 3, 40)) as token_id
       ,substring(data, 27 + 64 * 2, 40) as nft_contract_address
       ,cast(price as decimal(38, 0)) as amount_raw
       ,CAST(0 as double) as platform_fee_amount_raw
       ,cast(price as decimal(38, 0)) * CAST(element_at(fees, 1):`percentage` as decimal(38, 0))/1e8 as royalty_fee_amount_raw
       ,currency as currency_contract
       ,IF(no_of_items > CAST(1 AS DECIMAL(38, 0)), 'Bulk Purchase', 'Single Trade Item') as trade_type
       ,ROW_NUMBER() over (partition by evt_tx_hash
            ,bytea2numeric_v3(substring(data, 27 + 64 * 3, 40))
            ,price  Order by block_time) as row_num
    from {{ source('dew_polygon', 'dew_market_evt_Fulfilled') }} as dew_trades
        JOIN trades_data on dew_trades.evt_tx_hash = trades_data.tx_hash
    {% if not is_incremental() %}
    where evt_block_time >= '{{project_start_date}}'  -- dew first txn
    {% endif %}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
select
    'polygon' as blockchain
    ,'dew' as project
    ,'v1' as version
    ,er.block_time
    ,er.token_id
    ,n.name as collection
    ,er.amount_raw / power(10, t1.decimals) * p1.price as amount_usd
    ,case
        when erct2.evt_tx_hash is not null then 'erc721'
        when erc1155.evt_tx_hash is not null then 'erc1155'
    end as token_standard
    ,trade_type
    ,cast(1 as decimal(38, 0)) as number_of_items
    ,'Buy' as trade_category
    ,'Trade' as evt_type
    ,er.seller
    ,case
        when er.buyer = agg.contract_address then coalesce(erct2.to, erc1155.to)
        else er.buyer
    end as buyer
    ,er.amount_raw / power(10, t1.decimals) as amount_original
    ,er.amount_raw
    ,t1.symbol as currency_symbol
    ,er.currency_contract
    ,er.nft_contract_address
    ,er.project_contract_address
    ,agg.name as aggregator_name
    ,agg.contract_address as aggregator_address
    ,er.tx_hash
    ,er.evt_index as evt_index
    ,er.block_number
    ,tx.from as tx_from
    ,tx.to as tx_to
    ,er.platform_fee_amount_raw
    ,er.platform_fee_amount_raw / power(10, t1.decimals) as platform_fee_amount
    ,er.platform_fee_amount_raw / power(10, t1.decimals) * p1.price as platform_fee_amount_usd
    ,cast(0 as double) as platform_fee_percentage
    ,er.royalty_fee_amount_raw
    ,er.royalty_fee_amount_raw / power(10, t1.decimals) as royalty_fee_amount
    ,er.royalty_fee_amount_raw / power(10, t1.decimals) * p1.price as royalty_fee_amount_usd
    ,er.royalty_fee_amount_raw / er.amount_raw * 100 as royalty_fee_percentage
    ,NULL as royalty_fee_receive_address
    ,t1.symbol as royalty_fee_currency_symbol
    ,concat('dew-', er.tx_hash, '-', row_num, '-',
        er.nft_contract_address, '-', er.token_id, '-offer-1') as unique_trade_id
from events_raw as er
join source_polygon_transactions as tx
    on er.tx_hash = tx.hash
    and tx.block_number = er.block_number
left join ref_nft_aggregators as agg
    on agg.contract_address = tx.to
left join ref_tokens_nft as n
    on n.contract_address = er.nft_contract_address
left join ref_tokens_erc20 as t1
    on t1.contract_address = er.currency_contract
left join source_prices_usd as p1
    on p1.minute = date_trunc('minute', er.block_time)
    and p1.contract_address = er.currency_contract
left join {{ source('erc721_polygon','evt_transfer') }} as erct2
    on erct2.evt_block_time=er.block_time
    and er.nft_contract_address=erct2.contract_address
    and erct2.evt_tx_hash=er.tx_hash
    and erct2.tokenId=er.token_id
    and erct2.to=er.buyer
    {% if not is_incremental() %}
    -- smallest block number for source tables above
    and erct2.evt_block_number >= {{min_block_number}}
    {% endif %}
    {% if is_incremental() %}
    and erct2.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
left join {{ source('erc1155_polygon','evt_transfersingle') }} as erc1155
    on erc1155.evt_block_time=er.block_time
    and er.nft_contract_address=erc1155.contract_address
    and erc1155.evt_tx_hash=er.tx_hash
    and erc1155.id=er.token_id
    and erc1155.to=er.buyer
    {% if not is_incremental() %}
    -- smallest block number for source tables above
    and erc1155.evt_block_number >= '{{min_block_number}}'
    {% endif %}
    {% if is_incremental() %}
    and erc1155.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
