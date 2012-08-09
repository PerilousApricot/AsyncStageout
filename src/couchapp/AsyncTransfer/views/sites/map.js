function(doc) {
        if (doc.lfn && doc.source && doc.destination){
                emit(doc.source, 1);
                emit(doc.destination, 1);
                }
        }
