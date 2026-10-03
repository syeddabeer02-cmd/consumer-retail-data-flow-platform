"""Versioned synthetic funding policy, expressed with exact decimal arithmetic.

These illustrative rules are not Myntra's proprietary accounting policies.
All rates are fractions; all monetary results use HALF_UP cent rounding.
"""

from decimal import Decimal, ROUND_HALF_UP

POLICY_VERSION = "synthetic-v1"
CENT = Decimal("0.01")


def money(value):
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


def calculate(row):
    """Reference implementation used as an independent Spark test oracle."""

    def d(key):
        return Decimal(str(row[key]))

    q = d("quantity")
    gross = money(d("unit_price") * q)
    discount = money(gross * d("discount_rate"))
    net = gross - discount
    returned = money(net * d("return_rate"))
    eligible = net - returned
    commission = money(eligible * d("commission_rate"))
    tax = money(commission * d("tax_rate"))
    withholding = money(eligible * d("withholding_rate"))
    logistics = money(q * d("logistics_per_unit"))
    marketing = money(eligible * d("marketing_rate"))
    rebate = money(eligible * d("rebate_rate"))
    penalty = money(row["penalty"])
    adjustment = money(row["adjustment"])
    payable = money(
        eligible - commission - tax - withholding - logistics - marketing + rebate - penalty + adjustment
    )
    result = dict(
        gross_sales=gross,
        discount_amount=discount,
        net_sales=net,
        return_amount=returned,
        eligible_sales=eligible,
        commission_amount=commission,
        commission_tax=tax,
        withholding_amount=withholding,
        logistics_fee=logistics,
        marketing_fee=marketing,
        rebate_amount=rebate,
        penalty_amount=penalty,
        adjustment_amount=adjustment,
        net_payable=payable,
    )
    # Additional auditable measures: 35 named monetary/ratio calculations in total.
    result.update(
        total_deductions=commission + tax + withholding + logistics + marketing + penalty,
        total_credits=rebate + adjustment,
        revenue_per_unit=money(gross / q),
        discount_per_unit=money(discount / q),
        eligible_per_unit=money(eligible / q),
        commission_per_unit=money(commission / q),
        tax_per_unit=money(tax / q),
        withholding_per_unit=money(withholding / q),
        payable_per_unit=money(payable / q),
        discount_pct=money(discount / gross * 100) if gross else money(0),
        returns_pct=money(returned / net * 100) if net else money(0),
        commission_pct=money(commission / eligible * 100) if eligible else money(0),
        tax_pct=money(tax / commission * 100) if commission else money(0),
        withholding_pct=money(withholding / eligible * 100) if eligible else money(0),
        funding_margin=eligible - commission - marketing + rebate,
        pre_tax_payable=payable + tax + withholding,
        pre_adjustment_payable=payable - adjustment,
        cash_deductions=tax + withholding,
        commercial_deductions=commission + logistics + marketing + penalty,
        funded_revenue=eligible + rebate,
        effective_deduction_pct=money((eligible - payable) / eligible * 100) if eligible else money(0),
    )
    return result
