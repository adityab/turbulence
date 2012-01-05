// Abstract Agent schema

// TODO: Add some follow/unfollow fields, or use a separate graph DB for relationships
AgentSchema = new Schema({
    date: { 
        type: Date, 
        default: Date.now, 
        required: true
    },
    data: {
        contentType: {
            type: String,
            required: true
        },
        content: {
            type: Schema.ObjectId,
            required: true
        }
    }
});

AgentSchema.methods.create = function create(object, callback) {
    if(isBlank(object.data.contentType) || !object.data.content)
        return callback(new Error('AgentSchema.methods.create: Bad arguments'));
    else {
        this.data.contentType = object.data.contentType;
        this.data.content = object.data.content;
        return callback(null);
    }
};

// register model in mongoose
mongoose.model('agent', AgentSchema, 'agent');
