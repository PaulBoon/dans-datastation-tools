import os
import argparse
import unittest

from datastation.common.utils import is_sub_path_of, has_dirtree_pred, set_permissions, positive_int_argument_converter


class TestIsSubPathOf:
    def test_returns_true_for_child_dir(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        child = parent.mkdir('child')
        assert is_sub_path_of(child, parent)
        assert not is_sub_path_of(parent, child)

    def test_returns_true_for_child_file(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        child = parent.join('child')
        child.write('test')
        assert is_sub_path_of(child, parent)
        assert not is_sub_path_of(parent, child)

    def test_returns_true_for_child_file_in_subdir(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        subdir = parent.mkdir('subdir')
        child = subdir.join('child')
        child.write('test')
        assert is_sub_path_of(child, parent)
        assert not is_sub_path_of(parent, child)

    def test_returns_true_for_child_dir_in_subdir(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        subdir = parent.mkdir('subdir')
        child = subdir.mkdir('child')
        assert is_sub_path_of(child, parent)
        assert not is_sub_path_of(parent, child)

    def test_returns_false_for_non_child_dir(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        child = tmpdir.mkdir('child')
        assert not is_sub_path_of(child, parent)
        assert not is_sub_path_of(parent, child)

    def test_converts_child_to_absolute_path(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        parent.mkdir('child')
        os.chdir(parent)
        assert is_sub_path_of('child', os.path.abspath(parent))

    def test_converts_parent_to_absolute_path(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        child = parent.mkdir('child')
        os.chdir(parent)
        assert is_sub_path_of(os.path.abspath(child), '.')


class TestHasDirtreePred:

    def test_returns_true_if_all_files_and_dirs_satisfy_pred(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        parent.mkdir('subdir')
        parent.join('file').write('test')
        assert has_dirtree_pred(parent, lambda x: True)

    def test_returns_false_if_any_file_or_dir_does_not_satisfy_pred(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        parent.mkdir('subdir')
        parent.join('file').write('test')
        # One file does not satisfy the predicate
        assert not has_dirtree_pred(parent, lambda x: x != parent.join('file').strpath)

    def test_returns_false_if_any_dir_does_not_satisfy_pred(self, tmpdir):
        parent = tmpdir.mkdir('parent')
        subdir = parent.mkdir('subdir')
        parent.join('file').write('test')
        # One directory does not satisfy the predicate
        assert not has_dirtree_pred(parent, lambda x: x != subdir.strpath)


class TestSetPermissions:

    def test_sets_permissions_of_all_files_and_dirs(self, tmpdir):
        # Look up the group of the current user, so that we can set the group, as we cannot be sure that we have
        # permissions to set the group to root.
        group = os.stat(tmpdir.strpath).st_gid
        parent = tmpdir.mkdir('parent')
        parent.mkdir('subdir')
        parent.join('file').write('test')
        set_permissions(parent, file_mode=0o666, dir_mode=0o777, group=group)
        for root, dirs, files in os.walk(parent.strpath):
            assert oct(os.stat(root).st_mode)[-3:] == '777'
            for d in dirs:
                assert oct(os.stat(os.path.join(root, d)).st_mode)[-3:] == '777'
            for f in files:
                assert oct(os.stat(os.path.join(root, f)).st_mode)[-3:] == '666'


class TestPositiveIntArgumentConverter(unittest.TestCase):
    def test_positive_int_argument_converter(self):
        self.assertEqual(positive_int_argument_converter("5"), 5)
        self.assertEqual(positive_int_argument_converter("100"), 100)
        with self.assertRaises(argparse.ArgumentTypeError):
            positive_int_argument_converter("0")
        with self.assertRaises(argparse.ArgumentTypeError):
            positive_int_argument_converter("-5")
        with self.assertRaises(argparse.ArgumentTypeError):
            positive_int_argument_converter("abc")
