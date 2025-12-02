/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

MATCH (n:person{id: 1}) where n.born != 1000 RETURN n.name, n.born, n.id;
MATCH (a:person WHERE a.id = 1)-[e:know]->{6}(b:person) return e;
MATCH (n:person{id: 1, name: 'c'}) where n.born != 1000 RETURN n.name, n.born, n.id;
MATCH (n:person{id: 1}) where n.born < 1950 AND n.born > 1900 RETURN n.name, n.born, n.id;
MATCH (a:person {id: 1})-[e1:know]->(b:person)-[e2:acted_in]->(c:movie)
RETURN a.id, e1.weight, b.id, b.name, e2.role, c.title LIMIT 10;
MATCH (a:person {id: 1})-[e1:know]->(b:person)-[e2:acted_in]->(c:movie)
RETURN a.id, e1.weight, b.id, b.name, e2.role, c.title LIMIT 2;
MATCH (a:person {id: 1})-[e1:know]->{2}(b:person) return a.id, b.id, b.name;
MATCH (a:person WHERE a.id = 1)-[e:know]->{6}(b:person) return e;
MATCH p = (a:person WHERE a.id = 1) -[e:know]->{1, 6} (b:person) RETURN p;
MATCH (n:person{id: 1}) RETURN n.name, n.born, n.id;
MATCH (a:person {id: 3})-[b:acted_in]->(c:movie) RETURN b.role, b.act_year;
MATCH (a:person {id: 1})-[e1:know]->(b:person) return a.id, a.born, b.id, b.name, b.born;
MATCH (a:person {id: 1})-[e1:know]->{2}(b:person) return a.id, b.id, b.name, b.born;
MATCH (a:person {id: 1})-[e:know]->(b:person) return e.weight + b.born - b.id;
MATCH (n:person WHERE n.id = 1) -[e:know]->{2}(m:person) RETURN m.id;
MATCH (n:person{id:5})-[:know]->(m:person)<-[:know]-(k:person)-[:know]->(t:person)
RETURN n.id, m.id, k.id, t.id;
MATCH (n:person{id:1})-[:know]->(m:person)<-[:know]-(k:person) RETURN n.id, m.id, k.id;
MATCH (n:person{id:1}) RETURN n, n.name;
MATCH (a:person {id: 3})-[b:acted_in]->(c:movie) RETURN a,c;
MATCH (a:person {id: 1})-[e:know]->(c:person {id:3}) RETURN e;
MATCH (a:person {id: 3})-[e:acted_in]->(c:movie) RETURN e;
MATCH p=(a:person{id:1})-[e:know]->(b:person) RETURN a.id, a.name, b.id, b.name, p;
MATCH p=(a:person{id:1})-[e:know]->(b:person)-[e2:acted_in]->(c:movie) RETURN a.id, b.id, c.id, p;
MATCH (a:person {id: 1}) return a.born + a.born;
MATCH (a:person {id: 1})-[e:know]->(b:person) return a.born + b.born - b.id;
MATCH (n:person{id: 1}) RETURN n.name, n.born, n.id;
MATCH (a:person {id: 3})-[b:acted_in]->(c:movie) RETURN a.id, b.role, c.id LIMIT 10;
MATCH (a:person {id: 3})-[b:acted_in]->(c:movie)
RETURN a.name, a.id, b.role, c.id, c.title LIMIT 10;
MATCH (a:person {id: 3})-[b:acted_in]->(c:movie) WHERE c.id = 102
RETURN a.id, b.role, c.id LIMIT 10;
MATCH (a:person {id: 10000})-[b:acted_in]->(c:movie)
RETURN a.name, a.id, b.role, c.id, c.title LIMIT 10;
MATCH (a:person {id: 3})<-[b:know]-(c:person) RETURN a.name, a.id, b.weight, c.id, c.name;
MATCH (n:person{id: '1'}) RETURN n.name, n.born, n.id;
MATCH (a:person {id: '3'})-[b:acted_in]->(c:movie) RETURN a.id, b.role, c.id LIMIT 10;
MATCH (a:person {id: '3'})-[b:acted_in]->(c:movie)
RETURN a.name, a.id, b.role, c.id, c.title LIMIT 10;
MATCH (a:person {id: '3'})-[b:acted_in]->(c:movie) WHERE c.id = '102'
RETURN a.id, b.role, c.id LIMIT 10;
-- ISO-GQL labeled predicate extended examples
-- Multi-label with property specification
MATCH (n:person|movie{id: 1, name: 'x'}) RETURN n.id, n.name;
-- Property specification acts as labeled predicate conjunction
MATCH (n:person{id: 2, born: 1980, name: 'Tom'}) WHERE n.born = 1980 RETURN n.id, n.name, n.born;
-- Mixed patterns including multi-label edge (edge labels treated similarly)
MATCH (a:person{id:3})-[e:know|friend{id:10}]->(b:person|actor{name:'Alice'}) RETURN a.id, e.weight, b.id, b.name;
