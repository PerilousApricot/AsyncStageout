
function(doc) {
	complete_job = function(doc, req) {
        if ( doc['state'] != 'done' ) {
        	return false;
        }
        return true;
	}

	if(doc.lfn && complete_job(doc)){
		emit(doc.last_update, {"lfn": doc.lfn, "location": doc.source});
	}
}
