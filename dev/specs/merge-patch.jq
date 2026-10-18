# Applies a JSON Merge Patch (RFC 7386), passed as $patch using --slurpfile, to the input document.
# Objects are merged recursively, null removes a member, and any other value, including arrays, replaces the target.
def merge_patch($patch):
  if ($patch | type) == "object" then
    reduce ($patch | to_entries[]) as $entry (
      if type == "object" then . else {} end;
      if $entry.value == null then del(.[$entry.key]) else .[$entry.key] |= merge_patch($entry.value) end)
  else
    $patch
  end;

merge_patch($patch[0])
