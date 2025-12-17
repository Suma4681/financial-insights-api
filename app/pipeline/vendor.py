import re
import pandas as pd

def extract_vendor_name(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str)

    def _one(x: str):
        x_up = x.upper().strip()

        if re.search(r"\bCHECK\s*#", x_up):
            return "CHECK"

        m = re.search(r"ZELLE\s+PAYMENT\s+TO\s+(.+?)(\s+\d+|$)", x, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()[:60]

        m = re.search(r"ORIG\s+CO\s+NAME\s*:\s*(.+?)(\s+ORIG|\s+ID:|\s+DESC|$)", x, flags=re.IGNORECASE)
        if m:
            v = m.group(1).strip()
            return v[:60] if v else None

        tokens = re.findall(r"[A-Za-z0-9&'\-]+", x.strip())
        if not tokens:
            return None
        return " ".join(tokens[:3])[:60]

    return s.apply(_one)
