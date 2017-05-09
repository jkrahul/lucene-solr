package org.apache.solr.search.join;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;

import static org.apache.solr.common.params.CommonParams.TAG;

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

public class FiltersQParser extends QParser {

  private String getParamsLocalParamName() {
    return "params";
  }

  FiltersQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() throws SyntaxError {
    String[] params = localParams.getParams(getParamsLocalParamName());
    if(params==null || params.length == 0) {
      throw new SyntaxError("Params are not defined for filters query");
    }
    Set<String> tagsToExclude = new HashSet<>();
    String excludeTags = localParams.get("excludeTags");
    if (excludeTags != null) {
      tagsToExclude.addAll(StrUtils.splitSmart(excludeTags, ','));
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (String filter : params) {
      QParser parser = subQuery(filter, null);
      String tag = parser.getLocalParams().get(TAG);
      if (!tagsToExclude.contains(tag)) {
        Query query = parser.getQuery();
        builder.add(query, BooleanClause.Occur.MUST);
      }
    }
    String queryText = localParams.get(QueryParsing.V);
    // there is no child query, return parent filter from cache
    if (queryText != null && queryText.length() > 0) {
      QParser parser = subQuery(queryText, null);
      builder.add(parser.getQuery(), BooleanClause.Occur.MUST);
    }
    return builder.build();
  }

}






