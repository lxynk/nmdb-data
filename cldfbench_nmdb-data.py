import re
import pathlib
import collections

from csvw.dsv import reader

from pybtex import database
from clld.lib.bibtex import unescape

from cldfbench import Dataset as BaseDataset
from cldfbench import CLDFSpec

from pycldf.sources import Source

CODES = ['yes', 'no']


def norm_id(s):
    # "/" in ID is problematic because we might want to use ID as part of URLs.
    # Same goes for ":" in some contexts.
    return s.replace(':', '_').replace('/', '_')


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "nmdb-data"

    def cldf_specs(self):  # A dataset must declare all CLDF sets it creates.
        return CLDFSpec(dir=self.cldf_dir, module="StructureDataset") 

    def cmd_download(self, args):
        pass  # No need to implement. Raw data will be distributed with the repos.

    def schema(self, cldf):
        # Add custom additions to the default schema of a StructureDataset
        cldf.add_component('LanguageTable')
        cldf.add_component('ParameterTable')
        cldf.add_component('CodeTable')
        cldf.add_component(
            'ExampleTable',
            {
                'name': 'Source',
                'separator': ';',
                "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#source",
            },
            'Source_Comment',  # free text info about source
        )
        # We add a list-valued foreign key from Values to Examples.
        cldf.add_columns(
            'ValueTable',
            {
                'name': 'Example_IDs',
                'separator': ' ',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#exampleReference'}
        )

    def cmd_makecldf(self, args):
        self.schema(args.writer.cldf)

        bibdata = database.parse_file(str(self.raw_dir.joinpath('References.bib')))
        refs = set()
        for key, entry in bibdata.entries.items():
            src = Source.from_entry(key, entry)
            for k in src:
                src[k] = unescape(src[k])
            refs.add(src.id)
            args.writer.cldf.sources.add(src)

        def bibkey(string):
            for ref in refs:
                if ref in string:
                    return string
            return False

        glangs_by_iso = {lg.iso: lg for lg in args.glottolog.api.languoids() if lg.iso}

        for row in self.raw_dir.read_csv('Parameters.csv', dicts=True):
            if row['ID']:
                args.writer.objects['ParameterTable'].append(dict(
                    ID=norm_id(row['ID']),
                    Name=row['ID'],
                    Description=row['Parameter'],
                ))
                for code in CODES:
                    args.writer.objects['CodeTable'].append(dict(
                        ID='{}-{}'.format(norm_id(row['ID']), code),
                        Parameter_ID=norm_id(row['ID']),
                        Name=code,
                    ))
        langs = set()
        exs = collections.defaultdict(list)
        for p in self.raw_dir.glob('*-examples.csv'):
            print(p)
            for row in reader(p, dicts=True):
                if not all(k in row for k in  {'Language', 'Translation'}):
                    continue
                if row['ID']:
                    if row['Language'] not in langs:
                        glang = glangs_by_iso[row['Language']]
                        args.writer.objects['LanguageTable'].append(dict(
                            ID=row['Language'],
                            Name=glang.name,
                            Glottocode=glangs_by_iso[row['Language']].id,
                            Latitude=glang.latitude,
                            Longitude=glang.longitude))
                        langs.add(row['Language'])
                    args.writer.objects['ExampleTable'].append(dict(
                        ID=row['ID'],
                        Language_ID=row['Language'],
                        Primary_Text=row['Original'] or row['Morphemic'],
                        Analyzed_Word=row['Morphemic'].split(),
                        Gloss=row['Gloss'].split(),
                        Translated_Text=row['Translation'],
                        Source=[bibkey(row['Source'])] if bibkey(row['Source']) else None,
                        Source_Comment=row['Source'] if bibkey(row['Source']) is False else None
                    ))
                if row.get('Parameter'):
                    for pid in row.get('Parameter').split():
                        exs[norm_id(pid)].append(row['ID'])

        for p in self.raw_dir.glob('*-values.csv'):
            for row in reader(p, dicts=True):
                if all(row[k] for k in  {'ID', 'Language', 'Value'}) and any(c in row['Value'] for c in CODES):
                    code = [c for c in CODES if c in row['Value']][0]
                    if row['Language'] not in langs:
                        glang = args.glottolog.api.languoid(row['Language'])
                        args.writer.objects['LanguageTable'].append(dict(
                            ID=row['Language'],
                            Name=glang.name,
                            Glottocode=glangs_by_iso[row['Language']].id,
                            Latitude=glang.latitude,
                            Longitude=glang.longitude))
                        langs.add(row['Language'])
                    args.writer.objects['ValueTable'].append(dict(
                        ID=norm_id(row['ID']),
                        Language_ID=row['Language'],
                        Parameter_ID='_'.join(norm_id(row['ID']).split('_')[1:]),
                        Code_ID='_'.join(norm_id(row['ID']).split('_')[1:]) + '-' + code,
                        Value=row['Value'],
                        Example_IDs=exs.get(norm_id(row['ID']), []),
                    ))
