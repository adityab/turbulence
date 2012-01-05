// Abstract Post schema

var PostSchema = new Schema({
    // timestamp
    date: {
        type: Date,
        default: Date.now,
        required: true
    },
    // post author DBref
    author: {
        type: ObjectId, 
        ref: 'agent',
        required: true
    },
    // target agent - optional
    targetAgent: {
        type: ObjectId,
        ref: 'agent'
    },
    // target post - optional, think threaded conversations
    targetPost: {
        type: ObjectId,
        ref: 'post'
    },
    // visibility
    visibility: {
        // three modes
        mode: {
            type: String,
            enum: ['public', 'custom', 'private'],
            default: 'public',
            required: true
        },
        // array of privileged agents
        privileged: [{
            type: ObjectId,
            ref: 'agent'
        }]
    },
    data: {
        // content type of post
        contentType: {
            type: String,
            required: true
        },
        // content of post: we use arbitrary schema types, so we can't use a ref
        content: {
            type: ObjectId,
            required: true
        }
    }
});

PostSchema.methods.create = function create(object, callback) {
    if(!object.author || isBlank(object.visibility.mode) || isBlank(object.data.contentType) || !object.data.content)
        return callback(new Error("PostSchema.methods.create: Bad arguments"));
    else {
        this.author = object.author;
        this.visibility = object.visibility;
        this.data = object.data;
        return callback(null);
    }
};

mongoose.model('post', PostSchema, 'post');
