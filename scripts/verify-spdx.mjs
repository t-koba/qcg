export function verifySpdx(sbom) {
  if (
    sbom.spdxVersion !== "SPDX-2.3" ||
    sbom.SPDXID !== "SPDXRef-DOCUMENT" ||
    sbom.dataLicense !== "CC0-1.0" ||
    !/^https:\/\/[^#]+$/.test(sbom.documentNamespace)
  )
    throw new Error("invalid SPDX document identity");
  if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/.test(sbom.creationInfo.created))
    throw new Error("SPDX timestamp must use UTC seconds");
  const ids = new Set(["SPDXRef-DOCUMENT"]);
  for (const pkg of sbom.packages) {
    if (ids.has(pkg.SPDXID) || !/^SPDXRef-[A-Za-z0-9.-]+$/.test(pkg.SPDXID))
      throw new Error("duplicate/invalid SPDX ID");
    ids.add(pkg.SPDXID);
  }
  for (const edge of sbom.relationships)
    if (!ids.has(edge.spdxElementId) || !ids.has(edge.relatedSpdxElement))
      throw new Error("dangling SPDX relationship");
  if (
    !sbom.relationships.some(
      (edge) =>
        edge.spdxElementId === "SPDXRef-DOCUMENT" &&
        edge.relationshipType === "DESCRIBES" &&
        edge.relatedSpdxElement === "SPDXRef-Product",
    )
  )
    throw new Error("missing product identity");
  for (const ecosystem of ["cargo", "npm"])
    if (
      !sbom.packages.some((pkg) =>
        pkg.externalRefs?.some((ref) =>
          ref.referenceLocator.startsWith(`pkg:${ecosystem}/`),
        ),
      )
    )
      throw new Error(`missing ${ecosystem} inventory`);
  return ids.size;
}
