function(doc) {
        if (doc.state != 'failed' && doc.state != 'done' && doc.lfn) {
		emit([doc.user, doc.group, doc.role, doc.destination,
		      doc.source, doc.dn, doc.preserve_lfn], doc.lfn);
	}
}
