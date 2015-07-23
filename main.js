var fs = require("fs");
var path = require("path");
var dot = require('dot-object'); //for transforming queries for easier parsing
var merge = require("merge");
var Promise = require("bluebird");

//var Sealious = require("sealious");
var Sealious = require("../Sealious/lib/main.js");

var FileDatastore = new Sealious.ChipTypes.Datastore("file");

Sealious.ConfigManager.set_config("file_datastore", {
	"storage_dir": "./db"
});

function collection_to_filename(collection_name){
	var storage_dir_path = Sealious.ConfigManager.get_config().file_datastore.storage_dir;
	return path.resolve(storage_dir_path + "/" + collection_name + ".json");
}

function document_matches_query(document, unprocessed_query){
	//curently ignoring mongodb syntax's extras, like $gt etc. - we're just directly comparing two values
	if(typeof unprocessed_query == "object" && typeof document=="object"){
		var query = dot.object(unprocessed_query);
		for(var attribute_name in query){
			var match = document_matches_query(document[attribute_name], query[attribute_name])
			if(!match){
				return false;
			}
		}
		return true;
	}else{
		return document==unprocessed_query;
	}
}

function read_from_file(filename){
	if(!fs.existsSync(filename)){
		fs.writeFileSync(filename, "[]");
	}
	return require(filename);
}

function generate_sorting_function(sort_description){
	return function(a, b){
		for(var attribute_name in sort_description){
			if(a[attribute_name]>b[attribute_name]){
				return sort_description[attribute_name];
			}else if(a[attribute_name]<b[attribute_name]){
				return -1 * sort_description[attribute_name];
			}else{
				continue;
			}
		}
		return 0;
	}
}

function write_to_file(absolute_path, content){
	fs.writeFileSync(absolute_path, JSON.stringify(content));
}

function get_all_documents(collection_name){
	var filename = collection_to_filename(collection_name);
	return read_from_file(filename);
}

function apply_changes_to_document(document, unprocessed_changes){
	//`changes` is an object describing what changes to apply to the document, using [MongoDB's update syntax](http://docs.mongodb.org/manual/reference/method/db.collection.update/#example-update-specific-fields)
	//currently we only support the "$set" operator
	var changes = dot.object(unprocessed_changes["$set"]);
	return merge(document, changes);

}

FileDatastore.start = function(){
	var relative_path = Sealious.ConfigManager.get_config().file_datastore.storage_dir;
	var absolute_dir_path = path.resolve(relative_path);
	if(!fs.existsSync(absolute_dir_path)){
		fs.mkdirSync(absolute_dir_path);
	}
};

FileDatastore.find = function(collection_name, query, options, output_options){
	var all_entries = get_all_documents(collection_name);
	var result = all_entries.filter(function(document){
		return document_matches_query(document, query);
	});
	if(output_options && output_options.sort){
		var sorting_function = generate_sorting_function(output_options.sort);
		result.sort(sorting_function);
	}
	if(output_options && output_options.skip){
		result = result.splice(output_options.skip)
	}
	if(output_options && output_options.amount){
		result.splice(output_options.amount);
	}
	return Promise.resolve(result);
};

FileDatastore.insert = function(collection_name, to_insert, options){
	var all_entries = get_all_documents(collection_name);
	all_entries.push(to_insert);
	var filename = collection_to_filename(collection_name);
	write_to_file(filename, all_entries);
	return Promise.resolve(to_insert);
};

FileDatastore.update = function(collection_name, query, new_value){
	var all_entries = get_all_documents(collection_name);
	all_entries.forEach(function(document, index){
		if(document_matches_query(document, query)){
			all_entries[index] = apply_changes_to_document(document, new_value);
		}
	});
	return Promise.resolve();
};

FileDatastore.remove = function(collection_name, query, just_one){
	var all_entries = get_all_documents(collection_name);
	var filtered_entries = all_entries.filter(function(document){
		return document_matches_query(document, query);
	})
	var filename = collection_to_filename(collection_name);
	write_to_file(filename, all_entries);
	return Promise.resolve();
};

console.log(collection_to_filename("col"));