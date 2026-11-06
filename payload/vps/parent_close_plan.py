"""Pure, bounded allocation of a percentage across exact owned positions."""
import math
from decimal import Decimal, ROUND_FLOOR


def partial_close_plan(rows, spec, percent):
    if type(percent) not in (int,float) or not math.isfinite(percent) or not 0 < percent < 100:
        raise ValueError('Parent close percent must be greater than zero and below 100')
    def units(value):
        if type(value) not in (int,float) or not math.isfinite(value) or value<=0:
            raise ValueError('Invalid native close lot specification')
        result=Decimal(str(value))/step
        integer=result.to_integral_value()
        if abs(result-integer)>Decimal('0.0000001'):
            raise ValueError('Owned close volume does not match native lot step')
        return int(integer)
    value=spec.get('quantityStep')
    if type(value) not in (int,float) or not math.isfinite(value) or value<=0:
        raise ValueError('Invalid native close lot step')
    step=Decimal(str(value))
    minimum,maximum=units(spec.get('minQty')),units(spec.get('maxQty'))
    opened=[row for row in rows if row['status']=='open']
    if not opened:
        return dict(volumes={},volume=0.,remaining=0.)
    if len(opened)>200 or len({row['ticket'] for row in opened})!=len(opened):
        raise ValueError('Invalid owned partial-close position count')
    sizes=[units(row['volume']) for row in opened]
    total=sum(sizes)
    target=int((Decimal(total)*Decimal(str(percent))/100).to_integral_value(rounding=ROUND_FLOOR))
    if not 0<target<total:
        raise ValueError('Parent close percent is below the native lot step')
    # Each ticket may stay untouched, close fully, or leave a valid minimum.
    # Interval convolution avoids a table proportional to the account volume.
    layers=[[(0,0)]];options=[]
    for size in sizes:
        allowed=[(0,0)]
        if minimum<=size<=maximum:allowed.append((size,size))
        if minimum<=min(maximum,size-minimum):allowed.append((minimum,min(maximum,size-minimum)))
        options.append(allowed)
        candidates=sorted((lo+a,min(target,hi+b)) for lo,hi in layers[-1] for a,b in allowed if lo+a<=target)
        merged=[]
        for lo,hi in candidates:
            if merged and lo<=merged[-1][1]+1:merged[-1]=(merged[-1][0],max(merged[-1][1],hi))
            else:merged.append((lo,hi))
        if len(merged)>4096:
            raise ValueError('Owned partial-close allocation is too complex')
        layers.append(merged)
    if not any(lo<=target<=hi for lo,hi in layers[-1]):
        raise ValueError('Parent percentage cannot leave valid broker lot remainders')
    rest=target;parts=[0]*len(sizes)
    for index in range(len(sizes)-1,-1,-1):
        chosen=None
        for lo,hi in layers[index]:
            for a,b in reversed(options[index]):
                lower,upper=max(lo,rest-b),min(hi,rest-a)
                if lower<=upper:
                    chosen=upper;break
            if chosen is not None:break
        if chosen is None:raise ValueError('Owned partial-close allocation proof failed')
        parts[index]=rest-chosen;rest=chosen
    return dict(volumes={row['ticket']:float(step*part) for row,part in zip(opened,parts)},
        volume=float(step*target),remaining=float(step*(total-target)))
