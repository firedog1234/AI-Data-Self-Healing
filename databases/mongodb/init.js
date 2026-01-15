// MongoDB initialization script
// This script creates collections and sample unstructured data

// Switch to the database
db = db.getSiblingDB('studentdb');

// Create a collection for student documents with unstructured data
db.student_profiles.drop();
db.student_profiles.insertMany([
  {
    student_id: 1,
    email: 'alice.johnson@example.com',
    profile: {
      bio: 'Passionate about computer science and AI',
      interests: ['machine learning', 'web development', 'gaming'],
      social_links: {
        github: 'https://github.com/alice',
        linkedin: 'https://linkedin.com/in/alice'
      },
      extracurriculars: ['Hackathon Club', 'Women in Tech'],
      metadata: {
        last_updated: new Date(),
        source: 'student_portal'
      }
    }
  },
  {
    student_id: 2,
    email: 'bob.smith@example.com',
    profile: {
      bio: 'Mathematics enthusiast',
      interests: ['calculus', 'statistics', 'data science'],
      social_links: {
        github: 'https://github.com/bob'
      },
      extracurriculars: ['Math Club'],
      metadata: {
        last_updated: new Date(),
        source: 'student_portal'
      }
    }
  },
  {
    student_id: 3,
    email: 'charlie.brown@example.com',
    profile: {
      bio: 'Creative writer and literature lover',
      interests: ['creative writing', 'literature', 'poetry'],
      extracurriculars: ['Writing Club', 'Literary Magazine'],
      metadata: {
        last_updated: new Date(),
        source: 'student_portal'
      }
    }
  }
]);

// Create a collection for event logs (unstructured logging data)
db.event_logs.drop();
db.event_logs.insertMany([
  {
    event_type: 'login',
    user_email: 'alice.johnson@example.com',
    timestamp: new Date(),
    metadata: {
      ip_address: '192.168.1.100',
      user_agent: 'Mozilla/5.0',
      session_id: 'session_001'
    }
  },
  {
    event_type: 'course_view',
    user_email: 'bob.smith@example.com',
    course_code: 'CS101',
    timestamp: new Date(),
    metadata: {
      duration_seconds: 120,
      pages_viewed: 5
    }
  }
]);

// Create indexes for better query performance
db.student_profiles.createIndex({ student_id: 1 });
db.student_profiles.createIndex({ email: 1 });
db.event_logs.createIndex({ timestamp: -1 });
db.event_logs.createIndex({ event_type: 1 });

print('MongoDB initialization completed');

