package com.idowran.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BaseController {
	@Autowired
	private TransportClient client;
	
	@SuppressWarnings("rawtypes")
	@PostMapping("/add/book/novel")
	@ResponseBody
	public ResponseEntity add(
			@RequestParam("title") String title,
			@RequestParam("author") String author,
			@RequestParam("word_count") int wordCount,
			@RequestParam("publish_date") 
				@DateTimeFormat(pattern="yyyy-MM-dd HH:mm:ss")
				Date publishDate){
		// 添加文档
		
		try {
			// 构建json对象
			XContentBuilder content = XContentFactory.jsonBuilder().startObject()
				.field("title", title)
				.field("author", author)
				.field("word_count", wordCount)
				.field("publish_date", publishDate.getTime())
				.endObject();
			// 准备索引(索引，类型),设置文档对象，然后返回文档
			IndexResponse response = this.client.prepareIndex("book", "novel").setSource(content).get();
			// 返回文档的ID和状态码
			return new ResponseEntity<>(response.getId(), HttpStatus.OK);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// 500错误
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}	
	}
	
	@SuppressWarnings("rawtypes")
	@DeleteMapping("/delete/book/novel")
	@ResponseBody
	public ResponseEntity delete(@RequestParam("id") String id) {
		// 根据ID，删除文档
		
		// get(索引，类型，文档ID)
		DeleteResponse response = this.client.prepareDelete("book", "novel", id).get();
		
		return new ResponseEntity<>(response.getResult().toString(), HttpStatus.OK);
	}
	
	@SuppressWarnings("rawtypes")
	@PutMapping("/update/book/novel")
	@ResponseBody
	public ResponseEntity update(
			@RequestParam("id") String id,
			@RequestParam(name="title", required=false) String title,
			@RequestParam(name="author", required=false) String author
		){
		// 修改文档
		UpdateRequest update = new UpdateRequest("book", "noval", id);
		
		try {
			// 构建json对象
			XContentBuilder content = XContentFactory.jsonBuilder().startObject();
			if(title != null) content.field("title", title);
			if(author != null) content.field("author", author);
			content.endObject();
			
			update.doc(content);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// 500错误
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
		try {
			UpdateResponse response = this.client.update(update).get();
			return new ResponseEntity<>(response.getResult().toString(), HttpStatus.OK);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// 500错误
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@SuppressWarnings({ "rawtypes"})
	@GetMapping("/get/book/novel")
	@ResponseBody
	public ResponseEntity get(@RequestParam(name="id", defaultValue="") String id) {
		// 根据ID，查询文档
		if(id.isEmpty()) return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		
		// get(索引，类型，文档ID)
		GetResponse response = this.client.prepareGet("book", "novel", id).get();
		
		if(!response.isExists()) return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		
		return new ResponseEntity<>(response.getSource(), HttpStatus.OK);
	}
	
	@SuppressWarnings("rawtypes")
	@PostMapping("/query/book/novel")
	@ResponseBody
	public ResponseEntity query(
			@RequestParam(name="author", required=false) String author,
			@RequestParam(name="title", required=false) String title,
			@RequestParam(name="gt_word_count", defaultValue="0") int gtWordCount,
			@RequestParam(name="lt_word_count", required=false) Integer ltWordCount) {
		// 复合查询
		
		// 构建一个bool查询
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		
		if(author != null) {
			// 作者不为空，must匹配
			boolQuery.must(QueryBuilders.matchQuery("author", author));
		}
		if(title != null) {
			boolQuery.must(QueryBuilders.matchQuery("title", title));
		}
		
		// 构建一个范围查询
		RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("word_count");
		rangeQuery.from(gtWordCount);	// 设置下限
		if(ltWordCount != null && ltWordCount > 0) {
			rangeQuery.to(ltWordCount);	// 设置上限
		}
		// 把布尔查询和范围查询，用过滤器的方式结合起来。
		boolQuery.filter(rangeQuery);
		
		// 发起请求-构建请求体
		SearchRequestBuilder builder = this.client.prepareSearch("book")
			.setTypes("noval")
			.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			.setQuery(boolQuery)
			.setFrom(0)
			.setSize(10);
		// 在debug的时候，可以直接打印出来，是一个json格式的请求体信息
//		System.out.println(builder);
		
		// 执行请求，并返回结果，SearchResponse里面有个getHits方法，返回一个数组类型，数组的每一个元素可以是Map
		SearchResponse response = builder.get();
		
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		for (SearchHit hit: response.getHits()) {
			result.add(hit.getSourceAsMap());
		}
		return new ResponseEntity<>(result, HttpStatus.OK);
	}
	
}
