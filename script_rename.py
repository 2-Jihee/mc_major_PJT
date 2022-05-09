import re
from pathlib import Path

from data.data_loc import data_dir

patt_str = r'(\d{10}_\d{4}_)(?P<month>\d)_'

dir_path = Path(data_dir)
patt = re.compile(patt_str)
for fpath in dir_path.glob('mois/*/*/*.csv'):
    fname = fpath.name
    match_obj = patt.match(fname)
    if match_obj:
        month = match_obj.group('month').zfill(2)
        fname_new = re.sub(patt_str, fr'\g<1>{month}_', fname)
        fpath_new = fpath.parent / fname_new
        fpath.rename(fpath_new)
        print(f">>> Renamed '{fname}' to '{fname_new}'.")
