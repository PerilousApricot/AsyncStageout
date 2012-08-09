function(doc) {
	complete_job = function(doc) {
		if ( (doc['state'] == 'failed' ) || ( doc['state'] == 'done' ) ) {
			return true;
		}
		return false;
	
	}
	if(doc.lfn && complete_job(doc)){
		emit(doc.last_update, 
				{"lfn": doc.lfn,
				 "workflow": doc.workflow,
				 "location": doc.destination,
				 "checksum": doc.checksums,
				 "jobid": doc.jobid,
				 "retry_count": doc.retry_count.length+1,
				 "size": doc.size,
				 "state" : doc.state,
				 "preserve_lfn":doc.preserve_lfn,
                 "errors":doc.failure_reason});
	}
}
