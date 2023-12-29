insert into mobikwik_schema.UP_Txn_Table_zip (
  flag, date_flag, paylaterid, memberuid,
  user_status, super_nonsuper_zip,
  first_txn_date, last_txn_date, amountallocated,
  cumm_txn_cnt, cumm_txn_amount, final_txn_cnt,
  final_txn_amount
) with userpaylaterbalance as (
  select
    *,
    case when status = 1 then 'Active' when status = 2 then 'BLOCKED_AVAILABLE_AMOUNT_REACHED' when status in (3, 4) then 'Blocked, Due to non payment' when status = 5 then 'Blocked, Due to Fraud' when status = 8 then 'BLOCKED_ACCOUNT_MIGRATION' when status = 9 then 'Permanent Blocked' else 'Other' end as User_status
  from
    (
      select
        memberuid,
        max_by(status, accountactivationdate) as status,
        max_by(
          sanctionedAmount, accountactivationdate
        ) as sanctionedAmount,
        max_by(
          paylaterid, accountactivationdate
        ) as paylaterid,
        max_by(day, accountactivationdate) as day
      from
        paylater.userpaylaterbalance a
      where
        day in (
          select
            distinct updated_day
          from
            mobinew.daily_table_partition_tracking
          where
            day >= date_format(
              date_add(
                'month',
                -1,
                date('2023-12-05')
              ),
              '%Y%m%d'
            )
            and day < date_format(
              date_add(
                'day',
                0,
                date('2023-12-05')
              ),
              '%Y%m%d'
            )
            and schema in ('paylater')
            and "table" in ('userpaylaterbalance')
        )
      group by
        1
    )
),
superzipuser as (
  select
    *,
    case when flgsuperzipuser in (1, 3) then 'Super_Zip_User' else 'Non-Super_Zip_User' end as Super_Nonsuper_Zip
  from
    (
      select
        t1.*,
        t2.memberuid
      from
        paylater.superzipuser t1
        join paylater.userpaylaterbalance t2 on t1.paylaterid = t2.paylaterid
    )
  where
    day in (
      select
        distinct updated_day
      from
        mobinew.daily_table_partition_tracking
      where
        day >= date_format(
          date_add(
            'day',
            -1,
            date('2023-12-05')
          ),
          '%Y%m%d'
        )
        and day < date_format(
          date_add(
            'day',
            0,
            date('2023-12-05')
          ),
          '%Y%m%d'
        )
        and schema in ('paylater')
        and "table" in ('superzipuser')
    )
),
paylatertransaction_one_day as (
  select
    memberuid,
    max(paylaterid) as paylaterid,
    count(
      case when status = 800
      and txntype = 1
      and subtxntype in (1, 2, 3) then txnid end
    ) as Transaction_Cnt,
    sum(
      case when status = 800
      and txntype = 1
      and subtxntype in (1, 2, 3) then txnamount end
    ) as Amount
  from
    paylater.paylatertransaction
  where
    status = 800
    and txntype = 1
    and subtxntype in (1, 2, 3)
    and day >= date_format(
      date_add(
        'day',
        1,
        date_add(
          'year',
          -1,
          date('2023-12-05')
        )
      ),
      '%Y%m%d'
    )
    and day < date_format(
      date_add(
        'day',
        2,
        date_add(
          'year',
          -1,
          date('2023-12-05')
        )
      ),
      '%Y%m%d'
    )
  group by
    1
),
paylatertransaction_one_year as (
  select
    memberuid,
    max(paylaterid) as paylaterid,
    min(txndate) as First_txn_date,
    max(txndate) as Last_txn_date,
    count(
      case when status = 800
      and txntype = 1
      and subtxntype in (1, 2, 3) then txnid end
    ) as Transaction_Cnt,
    sum(
      case when status = 800
      and txntype = 1
      and subtxntype in (1, 2, 3) then txnamount end
    ) as Amount
  from
    paylater.paylatertransaction
  where
    status = 800
    and txntype = 1
    and subtxntype in (1, 2, 3)
    and day >= date_format(
      date_add(
        'day',
        2,
        date_add(
          'year',
          -1,
          date('2023-12-05')
        )
      ),
      '%Y%m%d'
    )
    and day < date_format(
      date_add(
        'day',
        -2,
        date('2023-12-05')
      ),
      '%Y%m%d'
    )
  group by
    1
),
Last_record as (
  select
    *
  from
    mobikwik_schema.UP_Txn_Table_zip
  where
    Date_Flag = date_format(
      date_add(
        'day',
        0,
        date_add(
          'year',
          -1,
          date('2023-12-05')
        )
      ),
      '%Y-%m-%d'
    )
)
select
  'Daily Level Data' as Flag,
  date_format(
    date_add(
      'day',
      1,
      date_add(
        'year',
        -1,
        date('2023-12-05')
      )
    ),
    '%Y%m%d'
  ) as Date_Flag,
  coalesce(
    a.paylaterid, b.paylaterid, c.paylaterid,
    d.paylaterid
  ) as paylaterid,
  coalesce(
    a.memberuid, b.memberuid, c.memberuid,
    d.memberuid
  ) as memberuid,
  coalesce(a.User_status, d.user_status) as User_status,
  coalesce(
    b.Super_Nonsuper_Zip, d.Super_Nonsuper_Zip
  ) as Super_Nonsuper_Zip,
  case when e.First_txn_date is null
  and d.First_txn_date is null then null when e.First_txn_date is null then d.First_txn_date when d.First_txn_date is null then e.First_txn_date when e.First_txn_date > d.First_txn_date then d.First_txn_date else e.First_txn_date end as First_txn_date,
  case when e.Last_txn_date is null
  and d.Last_txn_date is null then null when e.Last_txn_date is null then d.Last_txn_date when d.Last_txn_date is null then e.Last_txn_date when e.Last_txn_date < d.Last_txn_date then d.Last_txn_date else e.Last_txn_date end as Last_txn_date,
  coalesce(
    a.sanctionedAmount, d.amountallocated
  ) as sanctionedAmount,
  (
    coalesce(c.Transaction_Cnt, 0)+ coalesce(d.Cumm_Txn_Cnt, 0)
  ) as Cumm_Txn_Cnt,
  (
    coalesce(c.Amount, 0)+ coalesce (d.Cumm_Txn_Amount, 0)
  ) as Cumm_Txn_Amount,
  e.Transaction_Cnt as Final_Txn_cnt,
  e.Amount as Final_Txn_Amount
from
  userpaylaterbalance a full
  outer join superzipuser b on a.memberuid = b.memberuid full
  outer join paylatertransaction_one_day c on a.memberuid = c.memberuid full
  outer join paylatertransaction_one_year e on a.memberuid = e.memberuid full
  outer join Last_record d on a.memberuid = d.memberuid;
