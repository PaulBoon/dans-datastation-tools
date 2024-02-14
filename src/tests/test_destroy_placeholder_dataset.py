import datastation.dataverse.destroy_placeholder_dataset


def test_is_migration_file_returns_true_for_file_with_label_easy_migration_dot_zip():
    file_metadata = {'label': 'easy-migration.zip'}
    assert datastation.dataverse.destroy_placeholder_dataset.is_migration_file(file_metadata) == True


def test_is_migration_file_returns_true_for_file_with_directory_label_easy_migration():
    file_metadata = {'directoryLabel': 'easy-migration'}
    assert datastation.dataverse.destroy_placeholder_dataset.is_migration_file(file_metadata) == True


def test_is_migration_file_returns_false_for_file_with_label_not_easy_migration_dot_zip():
    file_metadata = {'label': 'not-easy-migration.zip'}
    assert datastation.dataverse.destroy_placeholder_dataset.is_migration_file(file_metadata) == False


def test_is_migration_file_returns_false_for_file_with_directory_label_not_easy_migration():
    file_metadata = {'directoryLabel': 'not-easy-migration'}
    assert datastation.dataverse.destroy_placeholder_dataset.is_migration_file(file_metadata) == False

