package org.apache.solr.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class BJQParserFiltersTest extends SolrTestCaseJ4 {
  
  private static final String[] klm = new String[] {"k", "l", "m"};
  private static final List<String> xyz = Arrays.asList("x", "y", "z");
  private static final String[] abcdef = new String[] {"a", "b", "c", "d", "e", "f"};
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema15.xml");
    createIndex();
  }
  
  public static void createIndex() throws IOException, Exception {
    int i = 0;
    List<List<String[]>> blocks = createBlocks();
    for (List<String[]> block : blocks) {
      List<XmlDoc> updBlock = new ArrayList<>();
      
      for (String[] doc : block) {
        String[] idDoc = Arrays.copyOf(doc,doc.length+2);
        idDoc[doc.length]="id";
        idDoc[doc.length+1]=Integer.toString(i);
        updBlock.add(doc(idDoc));
        i++;
      }
      //got xmls for every doc. now nest all into the last one
      XmlDoc parentDoc = updBlock.get(updBlock.size()-1);
      parentDoc.xml = parentDoc.xml.replace("</doc>", 
          updBlock.subList(0, updBlock.size()-1).toString().replaceAll("[\\[\\]]","")+"</doc>");
      assertU(add(parentDoc));
      
      if (random().nextBoolean()) {
        assertU(commit());
        // force empty segment (actually, this will no longer create an empty segment, only a new segments_n)
        if (random().nextBoolean()) {
          assertU(commit());
        }
      }
    }
    assertU(commit());
    assertQ(req("q", "*:*"), "//*[@numFound='" + i + "']");
    /*
     * dump docs well System.out.println(h.query(req("q","*:*",
     * "sort","_docid_ asc", "fl",
     * "parent_s,child_s,parentchild_s,grand_s,grand_child_s,grand_parentchild_s"
     * , "wt","csv", "rows","1000"))); /
     */
  }

  private static int id=0;
  private static List<List<String[]>> createBlocks() {
    List<List<String[]>> blocks = new ArrayList<>();
    for (String parent : abcdef) {
      List<String[]> block = createChildrenBlock(parent);
      block.add(new String[] {"parent_s", parent});
      blocks.add(block);
    }
    Collections.shuffle(blocks, random());
    return blocks;
  }

  private static List<String[]> createChildrenBlock(String parent) {
    List<String[]> block = new ArrayList<>();
    for (String child : klm) {
      block
          .add(new String[] {"child_s", child, "child_parent_s", parent});
    }
    Collections.shuffle(block, random());
    addGrandChildren(block);
    return block;
  }
  
  private static void addGrandChildren(List<String[]> block) {
    List<String> grandChildren = new ArrayList<>(xyz);
    // add grandchildren after children
    for (ListIterator<String[]> iter = block.listIterator(); iter.hasNext();) {
      String[] child = iter.next();
      assert child[0]=="child_s" && child[2]=="child_parent_s": Arrays.toString(child);
      String child_s = child[1];
      String parentchild_s = child[3];
      int grandChildPos = 0;
      boolean lastLoopButStillHasGrCh = !iter.hasNext()
          && !grandChildren.isEmpty();
      while (!grandChildren.isEmpty()
          && ((grandChildPos = random().nextInt(grandChildren.size() * 2)) < grandChildren
              .size() || lastLoopButStillHasGrCh)) {
        grandChildPos = grandChildPos >= grandChildren.size() ? 0
            : grandChildPos;
        iter.add(new String[] {"grand_s", grandChildren.remove(grandChildPos),
            "grand_child_s", child_s, "grand_parent_s", parentchild_s.substring(0,1)});
      }
    }
    // and reverse after that
    Collections.reverse(block);
  }
  

  private final static String beParents[] = new String[] {"//*[@numFound='2']",
      "//doc/arr[@name=\"parent_s\"]/str='b'",
      "//doc/arr[@name=\"parent_s\"]/str='e'"};

  private final static String eParent[] = new String[] {"//*[@numFound='1']",
      "//doc/arr[@name=\"parent_s\"]/str='e'"};

  private final static String elChild[] = new String[] {"//*[@numFound='1']",
      "//doc[" +
          "arr[@name=\"child_s\"]/str='l' and child::arr[@name=\"child_parent_s\"]/str='e']"};

  
  @Test
  public void testIntersectBqBjq() {
    assertQ(
        req("fq", "{!parent which=$pq v=$chq}\"", "q", "parent_s:(e b)", "chq",
            "child_s:l", "pq", "parent_s:[* TO *]"), beParents);
  }

  @Test
  public void testToParentFilters() {
    assertQ(
        req("fq", "{!parent filters=$child.fq which=$pq v=$chq}\"",
            "q",   "parent_s:(e b)",
            "child.fq", "+child_parent_s:e +child_s:l",
            "chq", "child_s:[* TO *]",
            "pq",  "parent_s:[* TO *]"), eParent);

    assertQ(
        req("fq", "{!parent filters=$child.fq which=$pq v=$chq}\"",
            "q",   "parent_s:(e b)",
            "child.fq", "child_parent_s:e",
            "child.fq", "child_s:l",
            "chq", "child_s:[* TO *]",
            "pq",  "parent_s:[* TO *]"), eParent);
  }

  @Test
  public void testFilters() {
    assertQ(
        req("q", "{!filters params=$child.fq v=$gchq}\"",
            "child.fq", "child_parent_s:e",
            "child.fq", "child_s:l",
            "gchq", "child_s:[* TO *]"), elChild);

    assertQ(
        req("q", "{!filters params=$child.fq excludeTags=firstTag v=$gchq}\"",
            "child.fq", "{!tag=firstTag}child_parent_s:e",
            "child.fq", "{!tag=secondTag}child_s:l",
            "gchq", "child_s:[* TO *]"), "//*[@numFound='6']");

    assertQ(
        req("q", "{!filters params=$child.fq excludeTags=secondTag v=$gchq}\"",
            "child.fq", "{!tag=firstTag}child_parent_s:e",
            "child.fq", "{!tag=secondTag}child_s:l",
            "gchq", "child_s:[* TO *]"), "//*[@numFound='3']");

    assertQ(
        req("q", "{!filters params=$child.fq excludeTags=firstTag,secondTag v=$gchq}\"",
            "child.fq", "{!tag=firstTag}child_parent_s:e",
            "child.fq", "{!tag=secondTag}child_s:l",
            "gchq", "child_s:[* TO *]"), "//*[@numFound='18']");
  }

  
}

