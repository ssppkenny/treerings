import os, re
from itertools import islice
import pandas as pd
from collections import namedtuple

HeadRecord = namedtuple('HeadRecord', ('filename', 'site_id', 'tree_type', 'elevation', 'lat', 'lon', 'min_year', 'max_year'))



def read_files(directory):
    headers = dict()
    files = os.listdir(directory)

    for filename in files:
        if os.path.isfile(directory + filename) and filename.endswith(".rwl"):
            with open(directory + filename, encoding="iso-8859-1") as infile:
                header = islice(infile, 3)
                headers[filename] = list(header)

    dfs = pd.read_fwf("/Users/ugxnbmikhs/code/treerings/tree-species-code.txt")
    species = set(dfs["Species"].tolist())

    return headers, species

def process_headers(headers, species):
    head_records = []
    for k, v in headers.items():
        found_species = re.findall(r"\s([A-Z]{4})\s", v[0])
        if list(filter(lambda x: x in species, found_species)):
            if not re.match(r"\w+?\s+-*\d+\s+\d+?.*", v[0]) and not re.match(r"\w+?\s+-*\d+\s+\d+?.*", v[1]):
                tree_type = found_species[-1]
                if len(v[1].rstrip()) > 0:
                    if re.match(r".*?\s+.*?\s+([\-]{0,1}[\dmM]+)\s+(\d+).*?(\d+).*?(\d+).*?", v[1]):
                        res = re.findall(r"([\dmM]+)", v[1].rstrip())
                        if len(res[-5:]) == 5:
                            a = res[-5:]
                            elev_str = re.sub(r'm', '', a[0], flags=re.IGNORECASE)
                            site_id = v[0][:7].rstrip()
                            site_id = re.findall(r"([^\s]+)\s*.*", site_id)[0]
                            try:
                                hr = HeadRecord(k, site_id, tree_type, int(elev_str) if elev_str else 0, int(a[1]),
                                                int(a[2]), int(a[3]), int(a[4]))
                            except:
                                try:

                                    elev_str = re.sub(r'm', '', a[1], flags=re.IGNORECASE)
                                    hr = HeadRecord(k, site_id, tree_type, int(elev_str) if elev_str else 0, int(a[2]),
                                                    int(a[3]), None, None)
                                except:
                                    pass

                            head_records.append(hr)
    return head_records



def read_data(header_record, directory):
    long_names = set()
    filename = directory + header_record.filename
    with open(filename, "r", encoding="iso-8859-1") as f:
        lines = f.readlines()
        data = lines[3:]
        d = dict()
        max_year = -float('inf')
        min_year = float('inf')
        trees = set()
        for l in data:
            entries = list(filter(lambda x: len(x) != 0, re.split(r'\s+', l)))
            if len(entries) >= 3:
                try:
                    tree_name = entries[0]
                    bi = 2
                    if len(tree_name) > 8:
                        tree_name=tree_name[:8]
                        tree_year = int(entries[0][8:])
                        bi = 1
                    else:
                        tree_year = int(entries[1])

                    trees.add(tree_name)
                    for i, k in enumerate(entries[bi:]):
                        measurement = int(k)
                        if not measurement in set([999,-999,9999,-9999, 9990]):
                            d[(tree_name, tree_year + i)] = int(k)
                            if tree_year + i > max_year:
                                max_year = tree_year + i
                            if tree_year + i < min_year:
                                min_year = tree_year + i
                except Exception as e:
                    print(e)
                    print(filename, l)

        series = []
        for t in trees:
            dd = dict()
            dd["site_id"] = header_record.site_id
            dd["tree_name"] = f"{header_record.site_id}_{t}"
            dd["tree_type"] = header_record.tree_type
            dd["lat"] = header_record.lat
            dd["lon"] = header_record.lon
            dd["elevation"] = header_record.elevation
            for y in range(-100, 2022):
                if (t, y) in d:
                    dd[str(y)] = d[(t, y)]

            series.append(pd.Series(name=t, data=dd))
        return series





if __name__ == '__main__':
    directory = "/Users/ugxnbmikhs/code/treerings/rwl/"
    headers, species = read_files(directory)
    head_records = process_headers(headers, species)
    counter = 0
    for header_record in head_records:
        myseries = read_data(header_record, directory)
        t = ['tree_name', 'site_id', 'tree_type', 'elevation', 'lat', 'lon']
        t.extend(list(map(str, range(1, 2022))))
        df = pd.DataFrame(data=myseries, columns=t)
        empty_df = pd.DataFrame(columns=t)

        if not os.path.exists(header_record.tree_type):
            os.mkdir(header_record.tree_type)
            empty_df.to_csv(f"{header_record.tree_type}/header.csv", columns=t)

        if counter == 0:
            df.to_csv(f"{header_record.tree_type}/{header_record.filename}.csv", columns=t)
        else:
            df.to_csv(f"{header_record.tree_type}/{header_record.filename}.csv", columns=t, header=False)
        counter += 1





#directory = "/Users/ugxnbmikhs/code/python/treerings/rwl/"
#read_raw_data(directory + "cana323.rwl")

# import cProfile, pstats, io
# from pstats import SortKey
#
# directory = "/Users/sergey/code/python/treerings/"
#
#
# pr = cProfile.Profile()
# pr.enable()
# # ... do something ...
# df = read_raw_data(directory + "cana136.rwl")
# print(df)
# pr.disable()
# s = io.StringIO()
# sortby = SortKey.CUMULATIVE
# ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
# ps.print_stats()
# print(s.getvalue())


