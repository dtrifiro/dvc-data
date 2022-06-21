import json
import logging
import posixpath
from typing import TYPE_CHECKING, Dict, Final, Iterable, Optional, Tuple

from dvc_objects.errors import ObjectFormatError
from dvc_objects.obj import Object
from funcy import cached_property

from ..hashfile.hash import hash_file
from ..hashfile.obj import HashFile

if TYPE_CHECKING:
    from dvc_objects.db import ObjectDB

    from ..hashfile.hash_info import HashInfo
    from ..hashfile.meta import Meta

logger = logging.getLogger(__name__)


class TreeError(Exception):
    pass


class MergeError(Exception):
    pass


def _try_load(
    odbs: Iterable["ObjectDB"],
    hash_info: "HashInfo",
) -> Optional["Object"]:
    for odb in odbs:
        if not odb:
            continue

        try:
            return Tree.load(odb, hash_info)
        except (FileNotFoundError, ObjectFormatError):
            pass

    return None


class Tree(HashFile):
    PARAM_RELPATH: Final = "relpath"

    def __init__(self):  # pylint: disable=super-init-not-called
        self.fs = None
        self.path = None
        self.hash_info = None
        self.oid = None
        self._dict: Dict[
            Tuple[str, ...], Tuple[Optional["Meta"], "HashInfo"]
        ] = {}

    @cached_property
    def _trie(self):
        from pygtrie import Trie

        return Trie(self._dict)

    def add(
        self, key: Tuple[str, ...], meta: Optional["Meta"], oid: "HashInfo"
    ):
        self.__dict__.pop("trie", None)
        self._dict[key] = (meta, oid)

    def get(
        self, key: Tuple[str, ...], default=None
    ) -> Optional[Tuple[Optional["Meta"], "HashInfo"]]:
        return self._dict.get(key, default)

    def digest(self):
        from dvc_objects.fs import MemoryFileSystem
        from dvc_objects.fs.utils import tmp_fname

        memfs = MemoryFileSystem()
        path = "memory://{}".format(tmp_fname(""))
        memfs.pipe_file(path, self.as_bytes())
        self.fs = memfs
        self.path = path
        _, self.hash_info = hash_file(path, memfs, "md5")
        assert self.hash_info.value
        self.hash_info.value += ".dir"
        self.oid = self.hash_info.value

    def _load(self, key, meta, hash_info):
        if hash_info and hash_info.isdir and not meta.obj:
            meta.obj = _try_load([meta.odb, meta.remote], hash_info)
            if meta.obj:
                for ikey, value in meta.obj.iteritems():
                    self._trie[key + ikey] = value
                    self._dict[key + ikey] = value

    def iteritems(self, prefix=None):
        kwargs = {}
        if prefix:
            kwargs = {"prefix": prefix}
            item = self._trie.longest_prefix(prefix)
            if item:
                key, (meta, hash_info) = item
                self._load(key, meta, hash_info)

        for key, (meta, hash_info) in self._trie.iteritems(**kwargs):
            self._load(key, meta, hash_info)
            yield key, (meta, hash_info)

    def shortest_prefix(self, *args, **kwargs):
        return self._trie.shortest_prefix(*args, **kwargs)

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        yield from (
            (key, value[0], value[1]) for key, value in self._dict.items()
        )

    def as_dict(self):
        return self._dict.copy()

    def as_list(self):
        from operator import itemgetter

        # Sorting the list by path to ensure reproducibility
        return sorted(
            (
                {
                    # NOTE: not using hash_info.to_dict() because we don't want
                    # size/nfiles fields at this point.
                    hi.name: hi.value,
                    self.PARAM_RELPATH: posixpath.sep.join(parts),
                }
                for parts, _, hi in self  # noqa: B301
            ),
            key=itemgetter(self.PARAM_RELPATH),
        )

    def as_bytes(self):
        return json.dumps(self.as_list(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_list(cls, lst):
        from ..hashfile.hash_info import HashInfo

        tree = cls()
        for _entry in lst:
            entry = _entry.copy()
            relpath = entry.pop(cls.PARAM_RELPATH)
            parts = tuple(relpath.split(posixpath.sep))
            hash_info = HashInfo.from_dict(entry)
            tree.add(parts, None, hash_info)
        return tree

    @classmethod
    def load(cls, odb, hash_info) -> "Tree":
        obj = odb.get(hash_info.value)

        try:
            with obj.fs.open(obj.path, "r") as fobj:
                raw = json.load(fobj)
        except ValueError as exc:
            raise ObjectFormatError(f"{obj} is corrupted") from exc

        if not isinstance(raw, list):
            logger.debug(
                "dir cache file format error '%s' [skipping the file]",
                obj.path,
            )
            raise ObjectFormatError(f"{obj} is corrupted")

        tree = cls.from_list(raw)
        tree.path = obj.path
        tree.fs = obj.fs
        tree.hash_info = hash_info
        tree.oid = hash_info.value

        return tree

    def filter(self, prefix: Tuple[str]) -> Optional["Tree"]:
        """Return a filtered copy of this tree that only contains entries
        inside prefix.

        The returned tree will contain the original tree's hash_info and
        path.

        Returns an empty tree if no object exists at the specified prefix.
        """
        tree = Tree()
        tree.path = self.path
        tree.fs = self.fs
        tree.hash_info = self.hash_info
        tree.oid = self.oid
        try:
            for key, (meta, oid) in self._trie.items(prefix):
                tree.add(key, meta, oid)
        except KeyError:
            pass
        return tree

    def get_obj(self, odb, prefix: Tuple[str]) -> Optional[Object]:
        """Return object at the specified prefix in this tree.

        Returns None if no object exists at the specified prefix.
        """
        _, hi = self._dict.get(prefix) or (None, None)
        if hi:
            return odb.get(hi.value)

        tree = Tree()
        depth = len(prefix)
        try:
            for key, (meta, entry_oid) in self._trie.items(prefix):
                tree.add(key[depth:], meta, entry_oid)
        except KeyError:
            return None
        tree.digest()
        return tree

    def ls(self, prefix=None):
        kwargs = {}
        if prefix:
            kwargs["prefix"] = prefix

        meta, hash_info = self._trie.get(prefix, (None, None))
        if hash_info and hash_info.isdir and meta and not meta.obj:
            raise TreeError

        ret = []

        def node_factory(_, key, children, *args):
            if key == prefix:
                list(children)
            else:
                ret.append(key[-1])

        self._trie.traverse(node_factory, **kwargs)

        return ret


def du(odb, tree):
    try:
        return sum(
            odb.fs.size(odb.oid_to_path(oid.value)) for _, _, oid in tree
        )
    except FileNotFoundError:
        return None


def _diff(ancestor, other, allow_removed=False):
    from dictdiffer import diff

    allowed = ["add"]
    if allow_removed:
        allowed.append("remove")

    result = list(diff(ancestor, other))
    for typ, _, _ in result:
        if typ not in allowed:
            raise MergeError(
                "unable to auto-merge directories with diff that contains "
                f"'{typ}'ed files"
            )
    return result


def _merge(ancestor, our, their):
    import copy

    from dictdiffer import patch

    our_diff = _diff(ancestor, our)
    if not our_diff:
        return copy.deepcopy(their)

    their_diff = _diff(ancestor, their)
    if not their_diff:
        return copy.deepcopy(our)

    # make sure there are no conflicting files
    _diff(our, their, allow_removed=True)

    return patch(our_diff + their_diff, ancestor)


def merge(odb, ancestor_info, our_info, their_info):
    from .. import load

    assert our_info
    assert their_info

    if ancestor_info:
        ancestor = load(odb, ancestor_info)
    else:
        ancestor = Tree()

    our = load(odb, our_info)
    their = load(odb, their_info)

    merged_dict = _merge(ancestor.as_dict(), our.as_dict(), their.as_dict())

    merged = Tree()
    for key, (meta, oid) in merged_dict.items():
        merged.add(key, meta, oid)
    merged.digest()

    return merged
