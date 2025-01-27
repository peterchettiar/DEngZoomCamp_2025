with 

source as (

    select * from {{ source('staging', 'greentaxi_trips') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed
