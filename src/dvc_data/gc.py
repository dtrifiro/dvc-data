from dvc_objects.executors import ThreadPoolExecutor

from dvc_data.transfer import _log_exceptions


def gc(
    odb, used, jobs=None, cache_odb=None, shallow=True, progress_callback=None
):
    from dvc_objects.errors import ObjectDBPermissionError

    from ._progress import QueryingProgress
    from .objects.tree import Tree

    if odb.read_only:
        raise ObjectDBPermissionError("Cannot gc read-only ODB")
    if not cache_odb:
        cache_odb = odb
    used_hashes = set()
    for hash_info in used:
        used_hashes.add(hash_info.value)
        if hash_info.isdir and not shallow:
            tree = Tree.load(cache_odb, hash_info)
            used_hashes.update(
                entry_obj.hash_info.value for _, entry_obj in tree
            )

    def _is_dir_hash(_hash):
        from .hashfile.hash_info import HASH_DIR_SUFFIX

        return _hash.endswith(HASH_DIR_SUFFIX)

    removed = False
    # hashes must be sorted to ensure we always remove .dir files first

    hashes_to_remove = {True: [], False: []}
    for hash_ in QueryingProgress(odb.all(jobs), name=odb.path):
        if hash_ in used_hashes:
            continue
        hashes_to_remove[_is_dir_hash(hash_)].append(hash_)

    def remover(hash_):
        path = odb.oid_to_path(hash_)
        if _is_dir_hash(hash_):
            # backward compatibility
            # pylint: disable=protected-access
            odb._remove_unpacked_dir(hash_)
        odb.fs.remove(path)
        return 1

    removed = 0
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        for dir_hash in (True, False):
            removed += sum(
                executor.imap_unordered(
                    _log_exceptions(remover), hashes_to_remove[dir_hash]
                )
            )
    return removed
