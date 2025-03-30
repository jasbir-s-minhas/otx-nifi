package com.opentext.nifi
//@Grab('com.fasterxml.jackson.core:jackson-databind:2.15.2')
//@Grab('com.jayway.jsonpath:json-path:2.7.0')

import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.node.*
import groovy.json.*
import com.jayway.jsonpath.*

class JsonComparator {

    ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    def comparisonRulesMap = [
            org: [
                    compare      : ["name", "realm", "status", "parentOrganization", "rootOrganization", "addresses[*].city", "phones[*].number", "authDomain", "organizationType", "public", "passwordPolicy", "authenticationPolicy", "attributes[*].key"],
                    ignore       : ["id", "creation", "version", "creator"],
                    substitute   : [
                            "realm"                : "realm",
                            "parentOrganization"   : ["realm": "realm", "id": null],
                            "rootOrganization"     : ["realm": "realm", "id": null],
                            "passwordPolicy"       : ["realm": "realm", "id": null],
                            "authenticationPolicy": ["realm": "realm", "id": null]
                    ],
                    ignore_paths : ["attributes[*].id"],
                    use_deepdiff : true,
                    debug        : true,
                    org_level_limit: 3
            ]
    ]

    List<Map> fullDiffReport = []

    void compare(String json1, String json2, String entityType = "org") {
        def rules = comparisonRulesMap[entityType]
        def tree1 = mapper.readTree(json1)
        def tree2 = mapper.readTree(json2)

        if (!tree1.isArray() || !tree2.isArray()) {
            println "Both JSON inputs must be arrays."
            return
        }

        tree1.eachWithIndex { node1, idx ->
            def node2 = idx < tree2.size() ? tree2.get(idx) : null
            if (node2 == null) {
                println "[DIFF] Missing element at index ${idx} in second array"
                fullDiffReport << [index: idx, path: "<missing>", old: node1.toString(), new: null]
                return
            }

            applyIgnorePaths(node1, rules.ignore_paths)
            applyIgnorePaths(node2, rules.ignore_paths)
            compareJsonNodes(node1, node2, rules, idx)
        }

        println "\n=== HUMAN-READABLE SUMMARY ==="
        fullDiffReport.each {
            println "[DIFF][${it.index}] ${it.path}: '${it.old}' vs '${it.new}'"
        }

        println "\n=== JSON PATCH FORMAT (Simplified) ==="
        println mapper.writerWithDefaultPrettyPrinter().writeValueAsString(fullDiffReport)
    }

    void compareJsonNodes(JsonNode node1, JsonNode node2, Map rules, int index) {
        rules.compare.each { path ->
            if (rules.ignore.contains(path)) return

            def val1 = safeJsonPathRead(node1, path)
            def val2 = safeJsonPathRead(node2, path)

            if (rules.use_deepdiff && val1 instanceof List && val2 instanceof List) {
                if (!val1.sort().equals(val2.sort())) {
                    logDiff(index, path, val1, val2)
                }
            } else {
                if (val1?.toString() != val2?.toString()) {
                    logDiff(index, path, val1, val2)
                }
            }
        }
    }

    void logDiff(int index, String path, def oldVal, def newVal) {
        println "[DIFF][${index}] Field '${path}' differs: ${oldVal} vs ${newVal}"
        fullDiffReport << [index: index, path: path, old: oldVal, new: newVal]
    }

    def safeJsonPathRead(JsonNode node, String path) {
        try {
            return JsonPath.read(node.toString(), "\$." + path)
        } catch (Exception e) {
            return null
        }
    }

    JsonNode substitute(JsonNode node, def rule) {
        if (rule instanceof Map) {
            ObjectNode newNode = JsonNodeFactory.instance.objectNode()
            rule.each { k, v ->
                if (v == null) {
                    newNode.putNull(k)
                } else if (node?.has(v)) {
                    newNode.set(k, node.get(v))
                }
            }
            return newNode
        } else if (rule instanceof String && node?.has(rule)) {
            return node.get(rule)
        } else {
            return node
        }
    }

    void applyIgnorePaths(JsonNode root, List<String> paths) {
        paths.each { path ->
            try {
                def result = JsonPath.parse(root.toString()).delete('$.' + path).jsonString()
                root.removeAll()
                root.setAll(mapper.readTree(result))
            } catch (Exception e) {
                println "[WARN] Failed to apply ignore path '${path}': ${e.message}"
            }
        }
    }

    static void main(String[] args) {
        def json1 = '''
        [
          {
            "name": "Org A",
            "realm": "dev",
            "status": "active",
            "id": "123",
            "addresses": [{"city": "Toronto"}],
            "phones": [{"number": "111-1111"}],
            "attributes": [{"key": "tier", "value": "gold", "id": "1"}]
          }
        ]
        '''

        def json2 = '''
        [
          {
            "name": "Org A",
            "realm": "prod",
            "status": "inactive",
            "id": "456",
            "addresses": [{"city": "Montreal"}],
            "phones": [{"number": "222-2222"}],
            "attributes": [{"key": "tier", "value": "gold", "id": "2"}]
          }
        ]
        '''

        new JsonComparator().compare(json1, json2, "org")
    }
}


