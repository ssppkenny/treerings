import pandas as pd, re, os
from collections import namedtuple

Header = namedtuple('Header', ('tree_type', 'site_id', 'site_name', 'investigators', 'elevation', 'latlon1', 'latlon2'))

def read_all_files():
    directory = "/Users/ugxnbmikhs/code/python/treerings/rwl/"
    for root, dirs, files in os.walk(directory):
        #series = []
        counter = 0
        for filename in files:
            if filename.endswith(".rwl"):
                try:
                    myseries = read_raw_data(directory + filename)
                    if not myseries is None:
                        #series.extend(myseries)
                        t = ['tree_type', 'site_id', 'site_name', 'investigators', 'elevation', 'latlon1', 'latlon2']
                        t.extend(list(map(str, range(0,2021))))
                        df = pd.DataFrame(data=myseries, columns=t)
                        if counter == 0:
                            df.to_csv(f"{filename}.csv", columns=t)
                        else:
                            df.to_csv(f"{filename}.csv", columns=t, header=False)
                        counter += 1
                except Exception as ex:
                    print("ERROR")
                    print(filename)
                    print(ex)



        #df = pd.DataFrame(data=series)
        #df.to_csv("out.csv")





def read_raw_data(filename):
    with open(filename, "r", encoding="iso-8859-1") as f:
        lines = f.readlines()
        m1 = re.match(r"[0-9a-zA-Z]{3}\s+1\s+.*", lines[0])
        m2 = re.match(r"[0-9a-zA-Z]{3}\s+2\s+.*", lines[1])
        m3 = re.match(r"[0-9a-zA-Z]{3}\s+3\s+.*", lines[2])

        if m1 and m2 and m3:
            tree_type = re.findall(r".*\s+([A-Z]{4})\s*", lines[0])[0]
            site_id = re.findall(r"([0-9a-zA-Z]{3}).+1\s+.*", lines[0])[0]
            site_name = re.findall(r"[0-9a-zA-Z]{3}-*\s+?1\s+\"*?([^\s]*\s*[^\s]*).*", lines[0])[0]
            investigators = re.findall(r"[0-9a-zA-Z]{3}.+3\s+(.*)", lines[2])[0]

            results = re.findall(r"[0-9a-zA-Z]{3}.+?2.*?(\-*\d+)M*.*?(\d{3,5})[- ]*?(\d{3,5}).*", lines[1])
            if results:
                elevation, latlon1, latlon2 = results[0]
                header = Header(tree_type, site_id, site_name, investigators, elevation, latlon1, latlon2)
                data = lines[3:]
                d = dict()
                max_year = -float('inf')
                min_year = float('inf')
                trees = set()
                for l in data:
                    entries = list(filter(lambda x: len(x) != 0, re.split(r'\s+', l)))
                    if len(entries) >= 3:
                        tree_name = entries[0]
                        trees.add(tree_name)
                        tree_year = int(entries[1])
                        for i, k in enumerate(entries[2:]):
                            try:
                                if int(k) != 999 and int(k) != -999:
                                    d[(tree_name, tree_year + i)] = int(k)
                                    if tree_year + i > max_year:
                                        max_year = tree_year + i
                                    if tree_year + i < min_year:
                                        min_year = tree_year + i
                            except:
                                pass


                series = []
                for t in trees:
                    dd = dict()

                    for y in range(0, 2022):
                        if (t, y) in d:
                            dd[str(y)] = d[(t, y)]
                    dd["site_id"] = header.site_id
                    dd["site_name"] = header.site_name
                    dd["species_code"] = tree_type

                    dd["investigators"] = header.investigators
                    dd["latlon1"] = header.latlon1
                    dd["latlon2"] = header.latlon2

                    #df = df.append(pd.Series(name=t, data=dd))
                    series.append(pd.Series(name=t, data=dd))
                return series


if __name__ == '__main__':
    read_all_files()
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


