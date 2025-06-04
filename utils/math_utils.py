def excelToFloat(x):
    try:
        return float(x)
    except:
        if type(x) is str:
            xstr = x
        else:
            xstr = x.__str__()

        if (xstr.strip().__len__() == 0): return 0;
        xm = xstr.replace('$', '')
        x0 = xm.replace('(', '-')
        x1 = x0.strip(')')
        x2 = x1.replace(',', '')
        return float(x2)
