import pathlib
import collections

from csvw.dsv import reader

from cldfbench import Dataset as BaseDataset
from cldfbench import CLDFSpec


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

    def cmd_makecldf(self, args):
        self.schema(args.writer.cldf)

        for row in self.raw_dir.read_csv('Parameters.csv', dicts=True):
            args.writer.objects['ParameterTable'].append(dict(
                ID=norm_id(row['ID']),
                Name=row['ID'],
                Description=row['Parameter'],
            ))

        exs = collections.defaultdict(list)
        for p in self.raw_dir.glob('*-examples.csv'):
            for row in reader(p, dicts=True):
                if row['ID']:
                    args.writer.objects['ExampleTable'].append(dict(
                        ID=row['ID'],
                        Language_ID=row['Language'],
                        Primary_Text=row['Original'] or row['Morphemic'],
                        Analyzed_Word=row['Morphemic'].split(),
                        Gloss=row['Gloss'].split(),
                        Translated_Text=row['Translation'],
                    ))
                if row.get('Parameter'):
                    exs[norm_id(row['Parameter'])].append(row['ID'])

        for p in self.raw_dir.glob('*-values.csv'):
            for row in reader(p, dicts=True):
                args.writer.objects['ValueTable'].append(dict(
                    ID=norm_id(row['ID']),
                    Language_ID=row['Language'],
                    Parameter_ID=norm_id(row['ID'].split(':')[-1]),
                    Value=row['Value'],
                    Example_IDs=exs.get(norm_id(row['ID']), []),
                ))

    def schema(self, cldf):
        """
        Add custom additions to the default schema of a StructureDataset
        """
        cldf.add_component('ParameterTable')
        cldf.add_component('ExampleTable')
        # We add a list-valued foreign key from Values to Examples.
        cldf.add_columns(
            'ValueTable',
            {
                'name': 'Example_IDs',
                'separator': ' ',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#exampleReference'}
        )
