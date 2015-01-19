The most important piece here is the TaxReader to create the suite of Ttax* taxonomy tables.

Ingest protein sequences into Accumulo.


Also has a Combiner-type iterator in the style of StatsCombiner called [ProteinStatsCombiner](src/main/java/edu/stevens/ProteinStatsCombiner.java). It counts the frequencies of each amino acid (and of gaps and degenerate characters and such).

[![Build Status](https://travis-ci.org/Stevens-GraphGroup/HBaaS-Ingester.svg)](https://travis-ci.org/Stevens-GraphGroup/HBaaS-Ingester)

[![Build Status](https://api.shippable.com/projects/547518e9d46935d5fbbe9951/badge?branchName=master)](https://app.shippable.com/projects/547518e9d46935d5fbbe9951/builds/latest)

`mvn package -DskipTests=true` to compile and build JARs.

`mvn test` to run tests.

### Snippets

#### Accumulo DB Table Snippets

    Ttax:
    1 :division|Unassigned [] 1421654426882 false 1
    1 :rank|no rank [] 1421654426882 false 1
    1 :scientific name|root [] 1421654426995 false 1
    1 :synonym|all [] 1421654426995 false 1
    10 :division|Bacteria [] 1421654426882 false 1
    10 :parent|135621 [] 1421654426882 false 1
    10 :rank|genus [] 1421654426882 false 1
    10 :scientific name|Cellvibrio [] 1421654426995 false 1
    10 :synonym|"Cellvibrio" Winogradsky 1929 [] 1421654426995 false 1
    10 :synonym|Cellvibrio (ex Winogradsky 1929) Blackall et al. 1986 em... TRUNCATED [] 1421654426995 false 1
    11 :authority|"Cellvibrio gilvus" Hulcher and King 1958 [] 1421654426995 false 1
    11 :division|Bacteria [] 1421654426882 false 1
    11 :equivalent name|Cellvibrio gilvus [] 1421654426995 false 1
    11 :parent|1707 [] 1421654426882 false 1
    11 :rank|species [] 1421654426882 false 1
    11 :scientific name|[Cellvibrio] gilvus [] 1421654426995 false 1
    
    TtaxT:
    authority|"Angiococcus disciformis" (Thaxter 1904) Jahn 1924 :38 [] 1421654426996 false 1
    authority|"Angiococcus" Jahn 1924 :42 [] 1421654426996 false 1
    authority|"Cellvibrio gilvus" Hulcher and King 1958 :11 [] 1421654426996 false 1
    ...
    blast name|bacteria :2 [] 1421654426996 false 1
    division|Bacteria :10 [] 1421654426883 false 1
    division|Bacteria :11 [] 1421654426883 false 1
    division|Bacteria :13 [] 1421654426883 false 1
    ...
    equivalent name|Methyliphilus methylitrophus :17 [] 1421654426996 false 1
    equivalent name|Methyliphilus methylotrophus :17 [] 1421654426996 false 1
    equivalent name|Methylophilus methylitrophus :17 [] 1421654426996 false 1
    genbank common name|eubacteria :2 [] 1421654426996 false 1
    genbank common name|fruiting gliding bacteria :29 [] 1421654426996 false 1
    ...
    misspelling|Shewanella putrifaciens :24 [] 1421654426996 false 1
    parent|13 :14 [] 1421654426883 false 1
    parent|131567 :2 [] 1421654426883 false 1
    parent|135621 :10 [] 1421654426883 false 1
    parent|16 :17 [] 1421654426883 false 1
    ...
    rank|family :39 [] 1421654426883 false 1
    rank|family :49 [] 1421654426883 false 1
    rank|genus :10 [] 1421654426883 false 1
    rank|genus :13 [] 1421654426883 false 1
    ...
    rank|no rank :1 [] 1421654426883 false 1
    rank|order :29 [] 1421654426883 false 1
    rank|species :11 [] 1421654426883 false 1
    rank|species :14 [] 1421654426883 false 1
    ...
    rank|superkingdom :2 [] 1421654426883 false 1
    scientific name|Angiococcus disciformis :38 [] 1421654426996 false 1
    scientific name|Archangium :47 [] 1421654426996 false 1
    scientific name|Archangium gephyra :48 [] 1421654426996 false 1
    scientific name|Azorhizobium :6 [] 1421654426996 false 1
    
    TtaxDeg:
    1 :deg [] 1421654426987 false 4
    10 :deg [] 1421654426987 false 6
    11 :deg [] 1421654426987 false 6
    13 :deg [] 1421654426987 false 5
    14 :deg [] 1421654426987 false 8
    16 :deg [] 1421654426987 false 7
    
    TtaxDegT:
    authority|"Angiococcus disciformis" (Thaxter 1904) Jahn 1924 :deg [] 1421654426994 false 1
    authority|"Angiococcus" Jahn 1924 :deg [] 1421654426994 false 1
    authority|"Cellvibrio gilvus" Hulcher and King 1958 :deg [] 1421654426994 false 1
    ...
    parent|32199 :deg [] 1421654426882 false 1
    parent|335928 :deg [] 1421654426882 false 1
    parent|39 :deg [] 1421654426882 false 4
    parent|39643 :deg [] 1421654426882 false 1
    parent|40 :deg [] 1421654426882 false 1
    parent|42 :deg [] 1421654426882 false 2
    parent|83461 :deg [] 1421654426882 false 1
    rank|family :deg [] 1421654426882 false 3
    rank|genus :deg [] 1421654426882 false 18
    rank|no rank :deg [] 1421654426882 false 1
    rank|order :deg [] 1421654426882 false 1
    rank|species :deg [] 1421654426882 false 31
    rank|superkingdom :deg [] 1421654426882 false 1
    scientific name|Angiococcus disciformis :deg [] 1421654426994 false 1
    scientific name|Archangium :deg [] 1421654426994 false 1
    
    TtaxFieldT:
    authority :deg [] 1421654426988 false 66
    blast name :deg [] 1421654426988 false 1
    division :deg [] 1421654426868 false 55
    equivalent name :deg [] 1421654426988 false 7
    genbank common name :deg [] 1421654426988 false 2
    genbank synonym :deg [] 1421654426988 false 1
    in-part :deg [] 1421654426988 false 6
    includes :deg [] 1421654426988 false 7
    misspelling :deg [] 1421654426988 false 6
    parent :deg [] 1421654426868 false 54
    rank :deg [] 1421654426868 false 55
    scientific name :deg [] 1421654426988 false 55
    synonym :deg [] 1421654426988 false 59
    type material :deg [] 1421654426988 false 101


