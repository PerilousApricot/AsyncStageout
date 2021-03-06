function() {
  // TODO this can be cleaned up with docForm

  var name = $("input[name=userCtxName]",this).val();
  var newProfile = {
    rand : Math.random().toString(), 
    nickname : $("input[name=nickname]",this).val(),
    email : $("input[name=email]",this).val(),
    url : $("input[name=url]",this).val()
  }, widget = $(this);

  // setup gravatar_url
  if (typeof hex_md5 == "undefined") {
    alert("creating a profile requires md5.js to be loaded in the page");
    return;
  }

  newProfile.gravatar_url = 'http://www.gravatar.com/avatar/'+hex_md5(newProfile.email || newProfile.rand)+'.jpg?s=40&d=identicon';

  // store the user profile on the user account document
  $.couch.userDb(function(db) {
    var userDocId = "org.couchdb.user:"+name;
    db.openDoc(userDocId, {
      success : function(userDoc) {
        userDoc["couch.app.profile"] = newProfile;
        db.saveDoc(userDoc, {
          success : function() {
            $$(widget).profile = newProfile;
            widget.trigger("profileReady", [newProfile]);
          }
        });
      }
    });
  });
  return false;
}