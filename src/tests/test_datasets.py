import pytest

from datastation.dataverse.datasets import Datasets
from datastation.dataverse.dataverse_client import DataverseClient


class TestDatasets:
    url = 'https://demo.archaeology.datastations.nl'
    cfg = {'server_url': url, 'api_token': 'xxx', 'safety_latch': 'ON', 'db': {}}

    def test_update_metadata(self, caplog, capsys):
        caplog.set_level('INFO')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title'}
        datasets.update_metadata(data, replace=True)
        assert capsys.readouterr().out == ('DRY-RUN: only printing command, not sending it...\n'
                                           f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
                                           "headers: {'X-Dataverse-key': 'xxx'}\n"
                                           "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y', 'replace': 'true'}\n"
                                           'data: {"fields": [{"typeName": "title", "value": "New title"}]}\n'
                                           '\n')
        assert len(caplog.records) == 1

        assert caplog.records[0].message == 'None'  # without dryrun: <Response [200]>
        assert caplog.records[0].funcName == 'update_metadata'
        assert caplog.records[0].levelname == 'INFO'

    def test_update_single_value_without_replace(self, caplog, capsys):
        caplog.set_level('INFO')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title'}
        with pytest.raises(Exception) as e:
            datasets.update_metadata(data, replace=False)
        assert str(e.value) == ("Single-value fields [title] must be replaced : "
                                "{'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title'}")
        assert capsys.readouterr().out == ''
        assert len(caplog.records) == 0

    def test_update_metadata_with_multi_value_field_without_replacing(self, caplog, capsys):
        caplog.set_level('INFO')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'dansRightsHolder[0]': 'me', 'dansRightsHolder[1]': "O\'Neill"}

        datasets.update_metadata(data, replace=False)
        assert (capsys.readouterr().out ==
                ('DRY-RUN: only printing command, not sending it...\n'
                 f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
                 "headers: {'X-Dataverse-key': 'xxx'}\n"
                 "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y'}\n"
                 'data: {"fields": [{"typeName": "dansRightsHolder", "value": ["me", "O\'Neill"]}]}\n'
                 '\n'))
        assert len(caplog.records) == 1
        assert (caplog.records[0].message == 'None')

    def test_update_metadata_with_multi_value_compound_field(self, caplog, capsys):
        caplog.set_level('INFO')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y',
                'author[0]authorName': 'me',
                'author[0]authorAffiliation': 'mine',
                'author[1]authorName': 'you',
                'author[1]authorAffiliation': 'yours'}

        datasets.update_metadata(data)

        assert (capsys.readouterr().out ==
                ('DRY-RUN: only printing command, not sending it...\n'
                 f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
                 "headers: {'X-Dataverse-key': 'xxx'}\n"
                 "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y'}\n"
                 'data: {"fields": [{"typeName": "author", "value": ['
                 '{"authorName": {"typeName": "authorName", "value": "me"}, '
                 '"authorAffiliation": {"typeName": "authorAffiliation", "value": "mine"}}, '
                 '{"authorName": {"typeName": "authorName", "value": "you"}, '
                 '"authorAffiliation": {"typeName": "authorAffiliation", "value": "yours"}}'
                 ']}]}\n\n'))

        assert len(caplog.records) == 1
        assert caplog.records[0].message == 'None'

    def test_update_metadata_with_single_value_compound_field(self, caplog, capsys):
        caplog.set_level('INFO')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        # found on the VMs but not on te datastations
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y',
                'socialScienceNotes@socialScienceNotesType': 'p',
                'socialScienceNotes@socialScienceNotesSubject': 'q',
                'socialScienceNotes@socialScienceNotesText': 'r'}
        # {"fields": [{"typeName": "socialScienceNotes", "value": {...}}]}
        # would cause a bad request
        # with  "Semantic error parsing dataset update Json: Empty value for field: Notes" in the server log
        # {"fields": [{"typeName": "socialScienceNotes", "value": [{...}]}]}
        # would cause an internal server error with an exception thrown by JsonParser.parseCompoundValue:
        # JsonArrayImpl cannot be cast to class javax.json.JsonObject
        #
        # comrades-dclDryLab and comrades-dclWetLab seem to be the only single-value compound fields
        # on dd-dtap/provisioning/files/custom-metadata-blocks/*.tsv

        with pytest.raises(Exception) as e:
            datasets.update_metadata(data)
        assert str(e.value) == ('Single-value compound fields '
                                "[socialScienceNotes@socialScienceNotesType] are not supported : {"
                                "'PID': 'doi:10.5072/FK2/8KQW3Y', "
                                "'socialScienceNotes@socialScienceNotesType': 'p', "
                                "'socialScienceNotes@socialScienceNotesSubject': 'q', "
                                "'socialScienceNotes@socialScienceNotesText': 'r'}")
        assert capsys.readouterr().out == ''
        assert len(caplog.records) == 0

    def test_update_metadata_with_too_many_values(self, caplog, capsys):
        caplog.set_level('DEBUG')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'xxx', 'dansRightsHolder[0]': 'me', 'rest.column': 'you'}
        with pytest.raises(Exception) as e:
            datasets.update_metadata(data, replace=True)
        assert str(e.value) == ("Invalid typeName [rest.column] : {"
                                "'PID': 'doi:10.5072/FK2/8KQW3Y', "
                                "'title': 'xxx', 'dansRightsHolder[0]': 'me', "
                                "'rest.column': 'you'}")
        assert capsys.readouterr().out == ''
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == 'DEBUG'
        assert caplog.records[0].message == ("{'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'xxx', 'dansRightsHolder[0]': "
                                             "'me', 'rest.column': 'you'}")
